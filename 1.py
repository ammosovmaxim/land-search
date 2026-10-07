import asyncio
import signal
import sys
import random
import re
from pathlib import Path
from typing import Any, List, Dict, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
from datetime import datetime

from pynspd import AsyncNspd

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class CheckStatus(Enum):
    """Статусы проверки кадастрового номера."""
    FOUND = "found"
    EMPTY = "empty"
    NOT_LAND_PLOT = "not_land_plot"
    TIMEOUT = "timeout"
    ERROR = "error"
    RATE_LIMITED = "rate_limited"
    NOT_FOUND = "not_found"


@dataclass
class CheckResult:
    """Результат проверки кадастрового номера."""
    cad_num: str
    status: CheckStatus
    error: Optional[str] = None
    ownership_type: Optional[str] = None


class AdaptiveRateLimiter:
    """
    Адаптивный rate limiter.

    - Замедляется при получении 429 (глобально, для всех задач).
    - Плавно ускоряется при серии успешных запросов.
    - Использует jitter, чтобы избежать синхронных "всплесков".
    """

    def __init__(
        self,
        base_rps: float = 1.0,
        min_rps: float = 0.2,
        max_rps: float = 3.0,
    ):
        self.base_rps = base_rps
        self.min_rps = min_rps
        self.max_rps = max_rps
        self.current_rps = base_rps
        self.last_request_time = 0.0
        self._lock = asyncio.Lock()
        self._consecutive_429 = 0
        self._consecutive_ok = 0
        self.total_429 = 0
        self.total_ok = 0

    async def acquire(self):
        """Ожидает, пока можно выполнить следующий запрос."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            interval = 1.0 / max(self.current_rps, 0.01)
            wait = interval - (now - self.last_request_time)
            if wait > 0:
                wait += random.uniform(0, 0.1)
                await asyncio.sleep(wait)
            self.last_request_time = asyncio.get_event_loop().time()

    async def on_success(self):
        """Регистрирует успешный запрос, плавно ускоряясь."""
        async with self._lock:
            self._consecutive_429 = 0
            self._consecutive_ok += 1
            self.total_ok += 1
            if self._consecutive_ok >= 20 and self.current_rps < self.max_rps:
                old_rps = self.current_rps
                self.current_rps = min(self.current_rps * 1.15, self.max_rps)
                self._consecutive_ok = 0
                logger.debug(
                    f"Ускорение: {old_rps:.2f} → {self.current_rps:.2f} rps"
                )

    async def on_rate_limit(self):
        """Регистрирует 429, резко замедляясь."""
        async with self._lock:
            self._consecutive_ok = 0
            self._consecutive_429 += 1
            self.total_429 += 1
            old_rps = self.current_rps
            self.current_rps = max(self.current_rps * 0.5, self.min_rps)
            logger.warning(
                f"Rate limit! Скорость: {old_rps:.2f} → {self.current_rps:.2f} rps "
                f"(подряд 429: {self._consecutive_429}, всего: {self.total_429})"
            )

    def snapshot(self) -> float:
        """Текущая скорость в rps."""
        return self.current_rps

    @property
    def consecutive_429(self) -> int:
        return self._consecutive_429


class GracefulExiter:
    """Контекстный менеджер для graceful shutdown."""

    def __enter__(self):
        self._original_sigint = signal.signal(signal.SIGINT, self._exit)
        self._original_sigterm = signal.signal(signal.SIGTERM, self._exit)
        self._exit_requested = False
        return self

    def _exit(self, signum, frame):
        if not self._exit_requested:
            self._exit_requested = True
            logger.warning("\n\nПолучен сигнал прерывания. Завершаем работу...")
            raise KeyboardInterrupt

    def __exit__(self, *args):
        signal.signal(signal.SIGINT, self._original_sigint)
        signal.signal(signal.SIGTERM, self._original_sigterm)

    @property
    def exit_requested(self) -> bool:
        return self._exit_requested


def log(msg: str, level: str = "info") -> None:
    """Упрощенное логирование для обратной совместимости."""
    getattr(logger, level, logger.info)(msg)


def extract_array(cad_num: str, quarter: str) -> str:
    """Извлекает код массива из полного кадастрового номера."""
    prefix = cad_num.rsplit(":", 1)[0]
    return prefix[len(quarter):]


def append_lines(file_path: Path, lines: List[str]) -> None:
    """Дозаписывает строки в файл, не перезаписывая прошлые данные."""
    if not lines:
        return
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")


def save_range_results(
    results_file: Path,
    quarter: str,
    array: str,
    range_start: int,
    range_end: int,
    found: List[str],
) -> None:
    """Сохраняет найденные номера после прохождения одного диапазона."""
    if not found:
        return
    header = f"# {quarter}{array}:{range_start}-{range_end} (найдено {len(found)})"
    append_lines(results_file, [header] + found)
    logger.info(
        f"Сохранено {len(found)} номеров диапазона "
        f"{range_start}-{range_end} → {results_file.name}"
    )


def is_404_error(error: Any) -> bool:
    """Проверяет, является ли ошибка HTTP 404 Not Found."""
    error_str = str(error).lower()
    return any((
        "404" in error_str,
        "not found" in error_str,
        "37163" in error_str,
    ))


def is_rate_limit_error(error: Any) -> bool:
    """Проверяет, является ли ошибка rate limiting (429)."""
    error_str = str(error).lower()
    return any((
        "429" in error_str,
        "too many requests" in error_str,
        "rate limit" in error_str,
    ))


def is_timeout_error(error: Any) -> bool:
    """Проверяет, является ли ошибка таймаутом."""
    error_str = str(error).lower()
    return any((
        "timeout" in error_str,
        "timed out" in error_str,
    ))


def _extract_retry_after(exc: Exception) -> Optional[float]:
    """
    Пытается вытащить Retry-After из ответа, если библиотека его пробрасывает.
    """
    response = getattr(exc, "response", None)
    if response is not None:
        try:
            ra = response.headers.get("Retry-After")
            if ra:
                return float(ra)
        except Exception:
            pass

    m = re.search(r"retry[\s\-_]*after[\s:]*([\d.]+)", str(exc), re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass

    return None


# ---------------------------------------------------------------------------
# Файлы настроек и прогресса
# ---------------------------------------------------------------------------

RANGES_FILE_NAME = "диапазоны.txt"
PROGRESS_FILE_NAME = "прогресс.txt"


def _parse_ranges_file(path: Path) -> List[Tuple[int, int]]:
    """Читает диапазоны из файла. Пустой список, если файла нет/ошибка."""
    if not path.exists():
        return []
    ranges: List[Tuple[int, int]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cleaned = line.replace("–", "-").replace("—", "-").replace(" ", "")
            if "-" not in cleaned:
                continue
            parts = cleaned.split("-", 1)
            try:
                r_start = int(parts[0])
                r_end = int(parts[1])
            except ValueError:
                continue
            if r_start >= 1 and r_end >= r_start:
                ranges.append((r_start, r_end))
    except Exception as e:
        logger.warning(f"Не удалось прочитать {path}: {e}")
        return []
    return ranges


def _save_ranges_file(path: Path, ranges: List[Tuple[int, int]]) -> None:
    """Сохраняет диапазоны в файл, чтобы подставить в следующий раз."""
    try:
        lines = ["# Диапазоны номеров участков. Формат: НАЧАЛО-КОНЕЦ"]
        for r_start, r_end in ranges:
            lines.append(f"{r_start}-{r_end}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception as e:
        logger.warning(f"Не удалось сохранить {path}: {e}")


def _read_progress(path: Path) -> Dict[str, Any]:
    """
    Читает прогресс из файла. Формат:
        array=003
        range_index=2
        range_start=5000
    """
    if not path.exists():
        return {}
    result: Dict[str, Any] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    except Exception as e:
        logger.warning(f"Не удалось прочитать {path}: {e}")
    return result


def _save_progress(
    path: Path,
    array: str,
    range_index: int,
    range_start: int,
) -> None:
    """Сохраняет текущую позицию сканирования."""
    try:
        lines = [
            "# Текущая позиция сканирования.",
            "# array — код массива, range_index — номер диапазона (с 1),",
            "# range_start — с какого номера продолжать внутри диапазона.",
            f"array={array}",
            f"range_index={range_index}",
            f"range_start={range_start}",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception as e:
        logger.warning(f"Не удалось сохранить {path}: {e}")


def _clear_progress(path: Path) -> None:
    """Сбрасывает прогресс (вызывается после успешного завершения)."""
    try:
        if path.exists():
            path.unlink()
    except Exception as e:
        logger.warning(f"Не удалось удалить {path}: {e}")


# ---------------------------------------------------------------------------
# Интерактивный ввод диапазонов
# ---------------------------------------------------------------------------

def ask_ranges(
    default_ranges: Optional[List[Tuple[int, int]]] = None,
    saved_file: Optional[Path] = None,
) -> Optional[List[Tuple[int, int]]]:
    """
    Спрашивает у пользователя диапазоны для сканирования.

    Возвращает список [(start, end), ...] или None, если пользователь
    решил выйти.
    """
    if default_ranges is None:
        default_ranges = [(1, 1000)]

    saved: List[Tuple[int, int]] = []
    if saved_file is not None:
        saved = _parse_ranges_file(saved_file)

    hint = saved if saved else default_ranges

    print("\n" + "=" * 60)
    print("ВВЕДИТЕ ДИАПАЗОНЫ НОМЕРОВ УЧАСТКОВ ДЛЯ СКАНИРОВАНИЯ")
    print("Формат: НАЧАЛО-КОНЕЦ, например: 1-1000")
    print("Можно ввести несколько диапазонов по одному в строке.")
    print("Пустая строка — закончить ввод и начать сканирование.")
    print("Команда 'exit' / 'q' / 'выход' — завершить работу скрипта.")
    print("=" * 60)
    if hint:
        print(f"Подсказка (Enter — использовать): {hint}")

    ranges: List[Tuple[int, int]] = []

    while True:
        try:
            raw = input("Диапазон: ").strip()
        except EOFError:
            break

        if raw.lower() in ("exit", "q", "quit", "выход"):
            return None

        if raw == "":
            if not ranges:
                if hint:
                    print(f"  ℹ️  Использую подсказку: {hint}")
                    return list(hint)
                print("  ⚠️  Диапазоны не заданы, использую 1-1000.")
                return list(default_ranges)
            break

        cleaned = raw.replace("–", "-").replace("—", "-").replace(" ", "")
        if "-" not in cleaned:
            print(f"  ⚠️  Не понял формат: {raw!r}. Нужно НАЧАЛО-КОНЕЦ, например 1-1000.")
            continue

        parts = cleaned.split("-", 1)
        try:
            r_start = int(parts[0])
            r_end = int(parts[1])
        except ValueError:
            print(f"  ⚠️  Не числа: {raw!r}. Попробуйте ещё раз.")
            continue

        if r_start < 1 or r_end < r_start:
            print(f"  ⚠️  Некорректный диапазон: {r_start}-{r_end}.")
            continue

        ranges.append((r_start, r_end))
        print(f"  ✅ Добавлен диапазон: {r_start}-{r_end}")

    ranges.sort()
    return ranges


def ask_start_point(
    arrays: List[str],
    quarter: str,
    saved_progress: Dict[str, Any],
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Спрашивает у пользователя, с какого массива и диапазона начинать.

    Возвращает (array_index, range_index, range_start), где:
        array_index — индекс в списке arrays (0-based)
        range_index — номер диапазона (0-based)
        range_start — с какого номера продолжать внутри диапазона

    (None, None, None) — пользователь решил выйти.
    """
    print("\n" + "=" * 60)
    print("ТОЧКА СТАРТА СКАНИРОВАНИЯ")
    print("=" * 60)

    if saved_progress:
        saved_array = saved_progress.get("array", "")
        saved_range_index = saved_progress.get("range_index", "")
        saved_range_start = saved_progress.get("range_start", "")
        if saved_array:
            print("Найден сохранённый прогресс:")
            print(f"  массив: {saved_array}")
            print(f"  диапазон №: {saved_range_index}")
            print(f"  начать с номера: {saved_range_start}")
            print()

    print("Введите номер массива, с которого начать.")
    print(f"Доступные массивы: {', '.join(arrays)}")
    print("Enter — начать с первого массива (или с сохранённого прогресса).")
    print("Команда 'exit' / 'q' / 'выход' — завершить работу.")
    print("=" * 60)

    # --- Выбор массива ---
    array_index = 0
    saved_array = saved_progress.get("array", "")
    if saved_array in arrays:
        try:
            answer = input(
                f"Массив (Enter — {saved_array} из прогресса): "
            ).strip()
        except EOFError:
            answer = ""
        if answer.lower() in ("exit", "q", "quit", "выход"):
            return None, None, None
        if answer == "":
            array_index = arrays.index(saved_array)
        else:
            if answer not in arrays:
                print(f"  ⚠️  Массив {answer!r} не найден в списке. Начну с первого.")
            else:
                array_index = arrays.index(answer)
    else:
        try:
            answer = input("Массив (Enter — первый): ").strip()
        except EOFError:
            answer = ""
        if answer.lower() in ("exit", "q", "quit", "выход"):
            return None, None, None
        if answer != "":
            if answer not in arrays:
                print(f"  ⚠️  Массив {answer!r} не найден в списке. Начну с первого.")
            else:
                array_index = arrays.index(answer)

    print(f"  → Начинаем с массива: {arrays[array_index]}")

    is_saved_array = (
        saved_progress
        and saved_array in arrays
        and array_index == arrays.index(saved_array)
    )

    # --- Выбор диапазона ---
    range_index = 0
    # Если в прогрессе есть range_index, предлагаем его как подсказку
    hint_range_index: Optional[int] = None
    if is_saved_array:
        try:
            hint_range_index = max(0, int(saved_progress.get("range_index", "1")) - 1)
        except (ValueError, TypeError):
            hint_range_index = None

    prompt = "Номер диапазона (Enter — первый, например 1 для первого): "
    if hint_range_index is not None:
        prompt = (
            f"Номер диапазона (Enter — {hint_range_index + 1} из прогресса): "
        )

    try:
        answer = input(prompt).strip()
    except EOFError:
        answer = ""
    if answer.lower() in ("exit", "q", "quit", "выход"):
        return None, None, None
    if answer != "":
        try:
            range_index = max(0, int(answer) - 1)
        except ValueError:
            print(f"  ⚠️  Не число: {answer!r}. Начну с первого диапазона.")
    elif hint_range_index is not None:
        range_index = hint_range_index

    # --- Стартовый номер внутри диапазона ---
    range_start: Optional[int] = None
    hint_range_start: Optional[int] = None
    if is_saved_array:
        try:
            hint_range_start = int(saved_progress.get("range_start", "0")) or None
        except (ValueError, TypeError):
            hint_range_start = None

    prompt = (
        "С какого номера продолжать внутри диапазона "
        "(Enter — с начала диапазона): "
    )
    if hint_range_start is not None:
        prompt = (
            f"С какого номера продолжать внутри диапазона "
            f"(Enter — {hint_range_start} из прогресса): "
        )

    try:
        answer = input(prompt).strip()
    except EOFError:
        answer = ""
    if answer.lower() in ("exit", "q", "quit", "выход"):
        return None, None, None
    if answer != "":
        try:
            range_start = int(answer)
            if range_start < 1:
                print("  ⚠️  Номер должен быть ≥ 1. Начну с начала диапазона.")
                range_start = None
        except ValueError:
            print(f"  ⚠️  Не число: {answer!r}. Начну с начала диапазона.")
    elif hint_range_start is not None:
        range_start = hint_range_start

    return array_index, range_index, range_start


# ---------------------------------------------------------------------------
# Проверка одного кадастрового номера
# ---------------------------------------------------------------------------

async def check_cadastral_number(
    nspd: AsyncNspd,
    cad_num: str,
    timeout: float,
    retries: int,
    retry_delay: float,
    semaphore: asyncio.Semaphore,
    rate_limiter: AdaptiveRateLimiter,
) -> CheckResult:
    """Проверяет один кадастровый номер с повторными попытками."""
    for attempt in range(retries + 1):
        async with semaphore:
            try:
                await rate_limiter.acquire()

                result = await asyncio.wait_for(nspd.search(cad_num), timeout=timeout)

                await rate_limiter.on_success()

                if not result:
                    return CheckResult(cad_num=cad_num, status=CheckStatus.EMPTY)

                for item in result:
                    props = item.properties.cast().options.model_dump()
                    if (props.get("land_record_type") or "").strip().lower() == "земельный участок":
                        return CheckResult(
                            cad_num=cad_num,
                            status=CheckStatus.FOUND,
                            ownership_type=props.get("ownership_type")
                        )

                return CheckResult(cad_num=cad_num, status=CheckStatus.NOT_LAND_PLOT)

            except asyncio.TimeoutError:
                if attempt == retries:
                    logger.debug(f"Таймаут для {cad_num} после {retries + 1} попыток")
                    return CheckResult(cad_num=cad_num, status=CheckStatus.TIMEOUT)

                wait_time = retry_delay * (2 ** attempt)
                logger.debug(f"Timeout для {cad_num}, повтор через {wait_time:.1f}с")
                await asyncio.sleep(wait_time)

            except Exception as exc:
                error_str = str(exc)

                # 404 / "объект не найден"
                if is_404_error(exc) or "37163" in error_str:
                    await rate_limiter.on_success()
                    logger.debug(f"Не найдено (404): {cad_num}")
                    return CheckResult(
                        cad_num=cad_num,
                        status=CheckStatus.EMPTY,
                        error="404 Not Found"
                    )

                # 429 Rate Limit
                if is_rate_limit_error(exc):
                    await rate_limiter.on_rate_limit()

                    if attempt == retries:
                        logger.warning(
                            f"Rate limit для {cad_num} после {retries + 1} попыток"
                        )
                        return CheckResult(
                            cad_num=cad_num, status=CheckStatus.RATE_LIMITED
                        )

                    retry_after = _extract_retry_after(exc)
                    if retry_after:
                        wait_time = retry_after
                    else:
                        wait_time = retry_delay * (2 ** attempt) * 2

                    wait_time += random.uniform(0, 1.5)

                    logger.debug(
                        f"429 для {cad_num}, пауза {wait_time:.1f}с "
                        f"(скорость: {rate_limiter.snapshot():.2f} rps)"
                    )
                    await asyncio.sleep(wait_time)
                    continue

                # Таймаут соединения
                if is_timeout_error(exc):
                    if attempt == retries:
                        logger.debug(f"Таймаут соединения для {cad_num}")
                        return CheckResult(cad_num=cad_num, status=CheckStatus.TIMEOUT)

                    wait_time = retry_delay * (2 ** attempt)
                    logger.debug(
                        f"Таймаут соединения для {cad_num}, повтор через {wait_time:.1f}с"
                    )
                    await asyncio.sleep(wait_time)
                    continue

                # Прочие ошибки
                if attempt == retries:
                    logger.error(f"Ошибка при проверке {cad_num}: {exc}")
                    return CheckResult(
                        cad_num=cad_num,
                        status=CheckStatus.ERROR,
                        error=error_str[:200]
                    )

                wait_time = retry_delay * (2 ** attempt)
                logger.debug(f"Ошибка для {cad_num}, повтор через {wait_time:.1f}с: {exc}")
                await asyncio.sleep(wait_time)

    return CheckResult(cad_num=cad_num, status=CheckStatus.TIMEOUT)


# ---------------------------------------------------------------------------
# Прогрев (один раз на весь процесс)
# ---------------------------------------------------------------------------

async def warmup_once(
    nspd: AsyncNspd,
    quarter: str,
    arrays: List[str],
    rate_limiter: AdaptiveRateLimiter,
) -> None:
    """Делает несколько пробных запросов, чтобы понять стартовую скорость."""
    if not arrays:
        return

    logger.info("Прогрев: проверяем скорость сервера...")

    tested = 0
    for arr in arrays[:3]:
        for probe in (1, 2):
            num = f"{quarter}{arr}:{probe}"
            try:
                await rate_limiter.acquire()
                await asyncio.wait_for(nspd.search(num), timeout=15.0)
                await rate_limiter.on_success()
                tested += 1
                await asyncio.sleep(0.8)
            except Exception as e:
                if is_rate_limit_error(e):
                    await rate_limiter.on_rate_limit()
                    logger.warning(
                        f"Сервер сразу отдаёт 429 на прогреве. "
                        f"Стартовая скорость снижена до {rate_limiter.snapshot():.2f} rps"
                    )
                    return
                await rate_limiter.on_success()
                tested += 1

    logger.info(
        f"Прогрев завершён ({tested} запросов). "
        f"Стартовая скорость: {rate_limiter.snapshot():.2f} rps"
    )


# ---------------------------------------------------------------------------
# Сканирование одного массива по заданным диапазонам
# ---------------------------------------------------------------------------

async def scan_array_full(
    nspd: AsyncNspd,
    quarter: str,
    array: str,
    ranges: List[Tuple[int, int]],
    batch_size: int = 50,
    timeout: float = 12.0,
    concurrency: int = 2,
    retries: int = 3,
    retry_delay: float = 1.0,
    stop_after_empty: int = 300,
    rate_limit: float = 1.0,
    rate_limiter: Optional[AdaptiveRateLimiter] = None,
    start_range_index: int = 0,
    start_range_from: Optional[int] = None,
    progress_file: Optional[Path] = None,
    results_file: Optional[Path] = None,
) -> List[str]:
    """
    Сканирует массив по заданным диапазонам номеров участков.
    Внутри каждого диапазона — автостоп по длинной пустой серии.

    start_range_index — с какого диапазона начать (0-based)
    start_range_from — с какого номера начать внутри первого сканируемого диапазона
    progress_file — куда писать текущую позицию после каждого батча
    results_file — куда сохранять найденные номера после каждого диапазона
    """
    found: List[str] = []
    no_owner: List[str] = []
    timeout_numbers: List[str] = []
    rate_limited_numbers: List[str] = []
    error_numbers: List[str] = []
    total_checked = 0
    errors = 0
    rate_limited = 0

    semaphore = asyncio.Semaphore(concurrency)
    if rate_limiter is None:
        rate_limiter = AdaptiveRateLimiter(
            base_rps=rate_limit,
            min_rps=0.2,
            max_rps=max(rate_limit * 2.0, 2.0),
        )

    output_no_owner = Path(__file__).parent / "номера_без_собственника.txt"
    output_timeouts = Path(__file__).parent / "timeout_номера.txt"
    output_rate_limited = Path(__file__).parent / "rate_limited_номера.txt"
    output_errors = Path(__file__).parent / "error_номера.txt"

    for r_idx, (range_start, range_end) in enumerate(ranges):
        # Пропускаем диапазоны до start_range_index
        if r_idx < start_range_index:
            logger.info(
                f"Пропуск диапазона {r_idx + 1}/{len(ranges)}: "
                f"{range_start}-{range_end} (по запросу)"
            )
            continue

        # Если это первый сканируемый диапазон и задан start_range_from —
        # начинаем не с range_start, а с указанного номера
        actual_start = range_start
        if r_idx == start_range_index and start_range_from is not None:
            actual_start = max(range_start, start_range_from)
            logger.info(
                f"\n>>> Диапазон {r_idx + 1}/{len(ranges)}: "
                f"{quarter}{array}:{range_start}-{range_end} "
                f"(начинаем с {actual_start})"
            )
        else:
            logger.info(
                f"\n>>> Диапазон {r_idx + 1}/{len(ranges)}: "
                f"{quarter}{array}:{range_start}-{range_end}"
            )

        empty_count = 0
        current = actual_start
        found_in_range: List[str] = []

        while current <= range_end:
            batch_end = min(current + batch_size - 1, range_end)
            cad_nums = [
                f"{quarter}{array}:{num}" for num in range(current, batch_end + 1)
            ]

            tasks = [
                check_cadastral_number(
                    nspd=nspd,
                    cad_num=cad_num,
                    timeout=timeout,
                    retries=retries,
                    retry_delay=retry_delay,
                    semaphore=semaphore,
                    rate_limiter=rate_limiter,
                )
                for cad_num in cad_nums
            ]
            results = await asyncio.gather(*tasks)

            batch_has_found = False
            batch_429 = 0

            for result in results:
                total_checked += 1

                if result.status == CheckStatus.FOUND:
                    found.append(result.cad_num)
                    found_in_range.append(result.cad_num)
                    empty_count = 0
                    batch_has_found = True
                    logger.info(f"  Найден: {result.cad_num}")
                    if result.ownership_type is None:
                        no_owner.append(result.cad_num)
                        logger.info(f"    (без собственника): {result.cad_num}")

                elif result.status in (CheckStatus.EMPTY, CheckStatus.NOT_LAND_PLOT):
                    empty_count += 1

                elif result.status == CheckStatus.TIMEOUT:
                    timeout_numbers.append(result.cad_num)
                    logger.debug(f"  Таймаут: {result.cad_num}")

                elif result.status == CheckStatus.RATE_LIMITED:
                    rate_limited += 1
                    batch_429 += 1
                    rate_limited_numbers.append(result.cad_num)
                    logger.warning(f"  Rate limit для: {result.cad_num}")

                elif result.status == CheckStatus.ERROR:
                    errors += 1
                    error_numbers.append(result.cad_num)
                    logger.debug(f"  Ошибка: {result.cad_num} - {result.error}")

            if batch_has_found or (current % (batch_size * 10) == 0):
                logger.info(
                    f"Проверено до {quarter}{array}:{batch_end} | "
                    f"найдено: {len(found)} | "
                    f"пустая серия: {empty_count} | "
                    f"ошибок: {errors} | "
                    f"rate limit: {rate_limited} | "
                    f"скорость: {rate_limiter.snapshot():.2f} rps"
                )

            # Стоп-кран при 429
            if batch_429 >= 3 or rate_limiter.consecutive_429 >= 2:
                current_rps = rate_limiter.snapshot()
                pause = 60 if current_rps < 0.5 else 30
                logger.warning(
                    f"Обнаружено {batch_429} ошибок 429 в батче. "
                    f"Общая пауза {pause}с (скорость: {current_rps:.2f} rps)"
                )
                await asyncio.sleep(pause)
                rate_limited = max(0, rate_limited - 5)

            if errors > 20:
                logger.warning(
                    f"Слишком много ошибок ({errors}), пауза 15с..."
                )
                await asyncio.sleep(15)
                errors = 0

            # Сохраняем прогресс после каждого батча
            if progress_file is not None:
                _save_progress(
                    progress_file,
                    array=array,
                    range_index=r_idx + 1,
                    range_start=batch_end + 1,
                )

            # Внутри одного диапазона — стоп по длинной пустой серии
            if empty_count >= stop_after_empty:
                logger.info(
                    f"\nДиапазон {range_start}-{range_end} исчерпан "
                    f"({empty_count} пустых подряд)."
                )
                break

            current = batch_end + 1

        # Сохраняем найденные номера после прохождения диапазона
        if results_file is not None:
            save_range_results(
                results_file=results_file,
                quarter=quarter,
                array=array,
                range_start=range_start,
                range_end=range_end,
                found=found_in_range,
            )

        # После завершения диапазона — сбрасываем start_range_from,
        # чтобы следующий диапазон шёл с начала
        start_range_from = None

    logger.info(
        f"\nМассив {quarter}{array}: всего проверено {total_checked}, "
        f"найдено {len(found)}, таймаутов {len(timeout_numbers)}, "
        f"rate limited {len(rate_limited_numbers)}, ошибок {len(error_numbers)}"
    )
    logger.info(
        f"Текущая скорость: {rate_limiter.snapshot():.2f} rps "
        f"(всего 429: {rate_limiter.total_429}, успешных: {rate_limiter.total_ok})"
    )

    if no_owner:
        logger.info(f"Найдено номеров без собственника: {len(no_owner)}")
        append_lines(output_no_owner, no_owner)

    if timeout_numbers:
        append_lines(output_timeouts, timeout_numbers)
        logger.info(f"Таймаут-номера сохранены в: {output_timeouts}")

    if rate_limited_numbers:
        append_lines(output_rate_limited, rate_limited_numbers)
        logger.info(f"Rate limited номера сохранены в: {output_rate_limited}")

    if error_numbers:
        append_lines(output_errors, error_numbers)
        logger.info(f"Ошибочные номера сохранены в: {output_errors}")

    return found


# ---------------------------------------------------------------------------
# Поиск массивов в квартале
# ---------------------------------------------------------------------------

async def discover_arrays(
    nspd: AsyncNspd,
    quarter: str,
    array_start: int = 1,
    array_end: int = 300,
    probe_numbers: Tuple[int, ...] = (1, 5),
    batch_size: int = 30,
    timeout: float = 8.0,
    concurrency: int = 2,
    retries: int = 3,
    retry_delay: float = 1.0,
    rate_limit: float = 1.0,
    rate_limiter: Optional[AdaptiveRateLimiter] = None,
) -> List[str]:
    """Ищет существующие массивы квартала батчами с прогрессом."""
    semaphore = asyncio.Semaphore(concurrency)
    if rate_limiter is None:
        rate_limiter = AdaptiveRateLimiter(
            base_rps=rate_limit,
            min_rps=0.2,
            max_rps=max(rate_limit * 2.0, 2.0),
        )
    found_arrays: Set[str] = set()
    all_arrays = [f"{value:03d}" for value in range(array_start, array_end + 1)]

    for probe in probe_numbers:
        if not all_arrays:
            break

        pending = [arr for arr in all_arrays if arr not in found_arrays]
        if not pending:
            break

        logger.info(
            f"\n--- Проба :{probe}, осталось проверить массивов: {len(pending)} ---"
        )

        pending_copy = pending.copy()

        for batch_start_idx in range(0, len(pending_copy), batch_size):
            batch = pending_copy[batch_start_idx: batch_start_idx + batch_size]

            if not batch:
                continue

            batch_from = batch[0]
            batch_to = batch[-1]
            checked = min(batch_start_idx + batch_size, len(pending_copy))

            cad_nums = [f"{quarter}{arr}:{probe}" for arr in batch]
            tasks = [
                check_cadastral_number(
                    nspd=nspd,
                    cad_num=cad_num,
                    timeout=timeout,
                    retries=retries,
                    retry_delay=retry_delay,
                    semaphore=semaphore,
                    rate_limiter=rate_limiter,
                )
                for cad_num in cad_nums
            ]
            results = await asyncio.gather(*tasks)

            newly_found = []
            batch_429 = 0
            for result in results:
                if result.status == CheckStatus.FOUND:
                    arr = extract_array(result.cad_num, quarter)
                    if arr not in found_arrays:
                        newly_found.append(arr)
                        found_arrays.add(arr)
                elif result.status == CheckStatus.RATE_LIMITED:
                    batch_429 += 1

            if newly_found:
                pending_copy = [
                    arr for arr in pending_copy if arr not in found_arrays
                ]

            logger.info(
                f"  [{batch_from}-{batch_to}] "
                f"прогресс: {checked}/{len(pending_copy) + len(found_arrays)} | "
                f"найдено массивов: {len(found_arrays)} | "
                f"новых в этом батче: {len(newly_found)} | "
                f"скорость: {rate_limiter.snapshot():.2f} rps"
            )

            await asyncio.sleep(0.5)

            if batch_429 >= 3 or rate_limiter.consecutive_429 >= 2:
                pause = 30
                logger.warning(
                    f"Обнаружено {batch_429} ошибок 429. Пауза {pause}с "
                    f"(скорость: {rate_limiter.snapshot():.2f} rps)"
                )
                await asyncio.sleep(pause)

            if not pending_copy:
                break

        logger.info(
            f"Проба :{probe} завершена -> всего массивов: {len(found_arrays)}"
        )

    return sorted(found_arrays)


# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------

async def main_async(
    quarter: str,
    default_max_end: int = 5000,
    concurrency: int = 1,
    batch_size: int = 50,
    rate_limit: float = 1.0,
) -> None:
    """Основная асинхронная функция."""
    array_start = 1
    array_end = 300

    script_dir = Path(__file__).parent
    arrays_file = script_dir / "найденные_массивы.txt"
    ranges_file = script_dir / RANGES_FILE_NAME
    progress_file = script_dir / PROGRESS_FILE_NAME

    logger.info("=" * 60)
    logger.info(f"ПОИСК МАССИВОВ В КВАРТАЛЕ {quarter}")
    logger.info("=" * 60)

    # Общий rate limiter на весь процесс
    shared_limiter = AdaptiveRateLimiter(
        base_rps=rate_limit,
        min_rps=0.2,
        max_rps=max(rate_limit * 2.0, 2.0),
    )

    async with AsyncNspd() as nspd:
        arrays = await discover_arrays(
            nspd=nspd,
            quarter=quarter,
            array_start=array_start,
            array_end=array_end,
            probe_numbers=(1, 5),
            batch_size=30,
            timeout=10.0,
            concurrency=concurrency,
            retries=3,
            retry_delay=1.0,
            rate_limit=rate_limit,
            rate_limiter=shared_limiter,
        )

        logger.info("\n" + "=" * 60)
        if not arrays:
            logger.warning(
                "Массивы не найдены. Проверьте формат quarter/диапазон массивов."
            )
            return

        logger.info(f"Найдено массивов: {len(arrays)}")
        logger.info(
            f"Массивы: {', '.join(arrays[:10])}" +
            (f"... и еще {len(arrays) - 10}" if len(arrays) > 10 else "")
        )
        arrays_file.write_text("\n".join(arrays) + "\n", encoding="utf-8")
        logger.info(f"Список массивов сохранён: {arrays_file}")

        # Один прогрев на весь процесс
        await warmup_once(nspd, quarter, arrays, shared_limiter)

        # Читаем сохранённый прогресс
        saved_progress = _read_progress(progress_file)

        # Спрашиваем точку старта
        start_array_index, start_range_index, start_range_from = ask_start_point(
            arrays=arrays,
            quarter=quarter,
            saved_progress=saved_progress,
        )
        if start_array_index is None:
            logger.info("Пользователь прервал работу до начала сканирования.")
            return

        all_plots: List[str] = []
        apply_to_all = False
        remembered_ranges: Optional[List[Tuple[int, int]]] = None

        for idx, array in enumerate(arrays, start=1):
            # Пропускаем массивы до стартового
            if idx - 1 < start_array_index:
                logger.info(
                    f"Пропуск массива {idx}/{len(arrays)}: {quarter}{array} "
                    f"(по запросу)"
                )
                continue

            logger.info("\n" + "=" * 60)
            logger.info(
                f"СКАНИРОВАНИЕ МАССИВА {idx}/{len(arrays)}: {quarter}{array}"
            )
            logger.info("=" * 60)

            # Диапазоны
            if apply_to_all and remembered_ranges:
                ranges = list(remembered_ranges)
                logger.info(f"Использую ранее заданные диапазоны: {ranges}")
            else:
                ranges = ask_ranges(
                    default_ranges=[(1, default_max_end)],
                    saved_file=ranges_file,
                )
                if ranges is None:
                    logger.info("Пользователь прервал работу. Сохраняем результаты...")
                    break

                remembered_ranges = list(ranges)
                _save_ranges_file(ranges_file, ranges)
                logger.info(f"Диапазоны сохранены в: {ranges_file}")

                if idx < len(arrays):
                    try:
                        answer = input(
                            "Применить эти диапазоны ко всем оставшимся массивам? (y/n): "
                        ).strip().lower()
                    except EOFError:
                        answer = "n"
                    if answer in ("y", "yes", "д", "да"):
                        apply_to_all = True
                        logger.info("ОК, диапазоны будут применены ко всем массивам.")

            # Применяем start_range_index / start_range_from только
            # для самого первого сканируемого массива
            if idx - 1 == start_array_index:
                effective_range_index = start_range_index
                effective_range_from = start_range_from
            else:
                effective_range_index = 0
                effective_range_from = None

            logger.info(f"Будут просканированы диапазоны: {ranges}")
            if effective_range_index > 0:
                logger.info(f"  (начинаем с диапазона №{effective_range_index + 1})")
            if effective_range_from is not None:
                logger.info(f"  (начинаем с номера {effective_range_from})")

            partial_file = (
                script_dir /
                f"найденные_участки_{quarter.replace(':', '_')}.txt"
            )

            plots = await scan_array_full(
                nspd=nspd,
                quarter=quarter,
                array=array,
                ranges=ranges,
                batch_size=batch_size,
                timeout=12.0,
                concurrency=concurrency,
                retries=2,
                retry_delay=1.0,
                stop_after_empty=300,
                rate_limit=rate_limit,
                rate_limiter=shared_limiter,
                start_range_index=effective_range_index,
                start_range_from=effective_range_from,
                progress_file=progress_file,
                results_file=partial_file,
            )
            all_plots.extend(plots)
            logger.info(f"Промежуточный результат сохранен: {partial_file}")

            if idx < len(arrays):
                logger.info("Пауза 3 секунды перед следующим массивом...")
                await asyncio.sleep(3)

        # После успешного завершения всех массивов — сбрасываем прогресс
        _clear_progress(progress_file)

    logger.info("\n" + "=" * 60)
    logger.info("ИТОГОВЫЙ РЕЗУЛЬТАТ:")
    logger.info(f"Найдено участков по кварталу {quarter}: {len(all_plots)}")

    if all_plots:
        final_file = (
            script_dir /
            f"все_участки_{quarter.replace(':', '_')}.txt"
        )
        final_file.write_text("\n".join(all_plots) + "\n", encoding="utf-8")
        logger.info(f"Все участки сохранены в: {final_file}")


def main() -> None:
    """Точка входа с обработкой аргументов командной строки."""
    import argparse

    parser = argparse.ArgumentParser(description="Сканер кадастровых номеров")
    parser.add_argument(
        "--quarter", default="77:01:000",
        help="Кадастровый квартал (например: 77:01:000)"
    )
    parser.add_argument(
        "--default-max-end", type=int, default=5000,
        help="Значение по умолчанию для диапазона 1-N, если ничего не введено"
    )
    parser.add_argument(
        "--concurrency", type=int, default=1,
        help="Количество одновременных запросов (осторожно с rate limit!)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50,
        help="Размер батча для сканирования"
    )
    parser.add_argument(
        "--rate-limit", type=float, default=1.0,
        help="Стартовая скорость запросов в секунду (рекомендуется 0.5-2)"
    )

    args = parser.parse_args()

    with GracefulExiter():
        try:
            asyncio.run(main_async(
                args.quarter,
                args.default_max_end,
                concurrency=args.concurrency,
                batch_size=args.batch_size,
                rate_limit=args.rate_limit,
            ))
        except KeyboardInterrupt:
            logger.info(
                "\nРабота прервана пользователем. Результаты частично сохранены."
            )
            sys.exit(0)
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()