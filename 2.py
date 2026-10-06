import requests
import time
import os
from pathlib import Path


def _load_token() -> str | None:
    """Загружает токен из переменной окружения или из файла .env."""
    token = os.environ.get("API_CLOUD_TOKEN", "").strip()
    if token:
        return token

    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return None

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key.strip() == "API_CLOUD_TOKEN":
            token = value.strip().strip('"').strip("'")
            if token:
                return token
    return None


# Токен api-cloud.ru берётся из переменной окружения API_CLOUD_TOKEN
# или из файла .env (не храните его в коде).
API_TOKEN = _load_token()

# Ключевые слова, по которым определяем аренду в обременениях
RENT_KEYWORDS = ["аренд", "субаренд", "наём", "наем"]

# Расшифровки основных ошибок API-Cloud
ERROR_MESSAGES = {
    "498": "TOKEN_NO_MONEY — недостаточно средств на балансе",
    "499": "WRONG_TOKEN_KEY — неверный формат токена (нужно 32 символа)",
    "502": "MISSING_REQUIRED_TOKEN_PARAMETER — отсутствует параметр token",
    "503": "TOKEN_NOT_REGISTERED_IN_THE_SYSTEM — токен не зарегистрирован",
    "504": "TOKEN_LOCKED_IN_THE_SYSTEM — токен заблокирован службой качества",
    "3":   "TOKEN_BLOCKED_BY_QUALITY_SERVICE — токен заблокирован",
    "2":   "TOKEN_TEHBLOCK — техническая блокировка токена",
    "456": "MAX_LIMIT — превышено количество одновременных соединений",
    "404": "TIME_MAX_CONNECT — ресурс не вернул ответ после 3 попыток",
    "1":   "API_SUSPENDED — работа метода временно приостановлена",
    "123": "IP_NOT_REGISTERED_IN_THE_SYSTEM — защита по IP",
}


def parse_api_error(data: dict) -> str | None:
    """
    Разбирает ответ API-Cloud на предмет ошибки.
    Возвращает текст ошибки или None, если ошибки нет.
    """
    error_code = data.get("error")
    if error_code:
        code_str = str(error_code)
        msg = ERROR_MESSAGES.get(code_str, f"API Error {code_str}")
        # Добавляем оригинальное сообщение, если оно есть
        original_msg = data.get("message", "")
        if original_msg:
            msg += f" — {original_msg}"
        return msg
    return None


def check_owner_and_rent(cadastral_number: str):
    """
    Проверяет объект по кадастровому номеру через метод object.
    """
    url = "https://api-cloud.ru/api/rosreestr.php"
    params = {
        "type": "object",
        "cadastr": cadastral_number,
        "token": API_TOKEN
    }

    try:
        response = requests.get(url, params=params, timeout=120)
        try:
            data = response.json()
        except ValueError:
            return {"has_owner": None, "has_rent": None,
                    "error": f"не JSON ответ (HTTP {response.status_code})"}

        # 1. Проверяем ошибки на уровне API-Cloud (токен, баланс и т.п.)
        api_error = parse_api_error(data)
        if api_error:
            return {"has_owner": None, "has_rent": None, "error": api_error}

        # 2. Проверяем status (на случай успешного ответа с ошибкой внутри)
        status = data.get("status")
        try:
            status_int = int(status)
        except (TypeError, ValueError):
            status_int = None
        if status_int != 200:
            return {"has_owner": None, "has_rent": None,
                    "error": f"статус API {status}"}

        # 3. Проверяем, найден ли объект
        if not data.get("found", False):
            return {"has_owner": None, "has_rent": None,
                    "error": "объект не найден"}

        obj = data.get("object", {})

        # 4. Проверяем наличие прав (собственность)
        rights = obj.get("rights") or []
        has_owner = len(rights) > 0

        # 5. Проверяем обременения на аренду
        encumbrances = obj.get("encumbrances") or []
        has_rent = False
        for enc in encumbrances:
            type_desc = (enc.get("typeDesc") or "").lower()
            if any(kw in type_desc for kw in RENT_KEYWORDS):
                has_rent = True
                break

        return {"has_owner": has_owner, "has_rent": has_rent, "error": None}

    except requests.exceptions.Timeout:
        return {"has_owner": None, "has_rent": None, "error": "таймаут 120 сек"}
    except requests.exceptions.ConnectionError as e:
        return {"has_owner": None, "has_rent": None, "error": f"ошибка соединения: {e}"}
    except Exception as e:
        return {"has_owner": None, "has_rent": None, "error": str(e)}


def filter_without_owners(input_file: str, output_file: str, delay: float = 1.0):
    """
    Фильтрует кадастровые номера, оставляя только те,
    у которых НЕТ собственника И НЕТ аренды.
    """
    if not os.path.exists(input_file):
        print(f"❌ ОШИБКА: Файл не найден: {input_file}")
        print(f"📁 Текущая папка: {os.getcwd()}")
        return

    if not API_TOKEN:
        print("❌ ОШИБКА: Не задан API_TOKEN!")
        print("   Задайте переменную окружения API_CLOUD_TOKEN или создайте файл .env")
        return

    with open(input_file, "r", encoding="utf-8") as f:
        cadastral_numbers = [line.strip() for line in f if line.strip()]

    total = len(cadastral_numbers)
    print(f"📂 Загружено номеров: {total}")
    print("=" * 60)

    no_owner_list = []   # нет ни собственности, ни аренды
    has_owner_list = []  # есть собственность
    has_rent_list = []   # есть аренда
    error_list = []      # ошибки (в т.ч. таймауты)

    for i, kn in enumerate(cadastral_numbers, 1):
        print(f"Обработка {i}/{total}: {kn}")
        res = check_owner_and_rent(kn)

        if res["error"]:
            error_list.append(kn)
            print(f"  ⚠️ {res['error']} — пропускаем")
        elif res["has_rent"]:
            has_rent_list.append(kn)
            print(f"  📄 В АРЕНДЕ — пропускаем")
        elif res["has_owner"]:
            has_owner_list.append(kn)
            print(f"  👤 ЕСТЬ собственник — пропускаем")
        else:
            no_owner_list.append(kn)
            print(f"  ✅ НЕТ собственника и НЕТ аренды — СОХРАНЯЕМ")

        time.sleep(delay)

    with open(output_file, "w", encoding="utf-8") as f:
        for kn in no_owner_list:
            f.write(kn + "\n")

    print("\n" + "=" * 60)
    print("✅ ГОТОВО!")
    print(f"Всего проверено:        {total}")
    print(f"  📌 БЕЗ собственника:  {len(no_owner_list)}")
    print(f"  📌 С собственником:   {len(has_owner_list)}")
    print(f"  📌 В аренде:          {len(has_rent_list)}")
    print(f"  ⚠️ Ошибок/не найдено: {len(error_list)}")
    print(f"Результат сохранён в:   {output_file}")
    print("=" * 60)

    if error_list:
        print(f"\n⚠️ Номера с ошибками (первые 10):")
        for kn in error_list[:10]:
            print(f"  - {kn}")
        if len(error_list) > 10:
            print(f"  ... и ещё {len(error_list) - 10}")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_filename = os.path.join(script_dir, "номера_без_собственника.txt")
    output_filename = os.path.join(script_dir, "участки_без_собственника.txt")

    print(f"📁 Папка со скриптом: {script_dir}")
    print(f"📂 Входной файл: {input_filename}")
    print(f"📂 Выходной файл: {output_filename}\n")

    filter_without_owners(input_filename, output_filename, delay=1.0)