import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from datetime import date, datetime

try:
    from pynspd import AsyncNspd
except ImportError:
    print("❌ Библиотека pynspd не установлена.")
    print("Установите её командой: pip install pynspd")
    sys.exit(1)


# ----------------------------- НАСТРОЙКИ -----------------------------
INPUT_FILE = "участки_без_собственника.txt"
OUTPUT_FOLDER = "geojson_results"

# Сколько запросов выполнять параллельно. Подбирайте аккуратно:
# 3-5 — безопасно, 10+ — риск получить бан от НСПД.
CONCURRENT_LIMIT = 5

# Пауза между запросами внутри одного воркера (сек).
DELAY_BETWEEN_REQUESTS = 0.5

# Как часто сохранять промежуточный отчёт (в обработках).
SAVE_EVERY = 50

# Предупреждение, если итоговый GeoJSON больше этого размера (МБ).
SIZE_WARNING_MB = 50
# ---------------------------------------------------------------------


def json_serializer(obj):
    """Преобразует объекты, не поддерживаемые JSON, в строки."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def format_progress(current: int, total: int) -> str:
    """Простой текстовый прогресс-бар."""
    filled = int(20 * current / total) if total else 0
    bar = "█" * filled + "░" * (20 - filled)
    percent = (current / total * 100) if total else 0
    return f"[{bar}] {percent:5.1f}%  {current}/{total}"


async def get_geojson_for_one(nspd, cad_number: str, output_folder: Path) -> dict:
    """
    Получает GeoJSON для одного кадастрового номера и сохраняет в файл.
    Возвращает словарь результата.
    """
    try:
        # Поиск участка по кадастровому номеру
        feat = await nspd.find(cad_number)

        if not feat:
            return {"cad_number": cad_number, "status": "not_found",
                    "error": "Объект не найден в НСПД"}

        if not feat.geometry:
            return {"cad_number": cad_number, "status": "no_geometry",
                    "error": "У объекта нет геометрии"}

        # Получаем геометрию в формате GeoJSON
        try:
            geojson_geometry = feat.geometry.model_dump()
        except Exception as e:
            return {"cad_number": cad_number, "status": "error",
                    "error": f"Не удалось получить геометрию: {e}"}

        # Проверка валидности геометрии
        if not isinstance(geojson_geometry, dict) or not geojson_geometry.get("type"):
            return {"cad_number": cad_number, "status": "error",
                    "error": "Пустая или невалидная геометрия"}

        # Получаем свойства (мягко, с логированием сбоя)
        properties = {}
        try:
            raw_properties = feat.properties.cast().options.model_dump()
            for key, value in raw_properties.items():
                if value is None:
                    continue
                if isinstance(value, (datetime, date)):
                    properties[key] = value.isoformat()
                elif isinstance(value, (int, float, str, bool)):
                    properties[key] = value
                else:
                    properties[key] = str(value)
        except (AttributeError, TypeError) as e:
            print(f"   ⚠️ {cad_number}: не удалось получить properties ({e})")

        # Формируем полноценный GeoJSON Feature
        full_feature = {
            "type": "Feature",
            "properties": {"cad_number": cad_number, **properties},
            "geometry": geojson_geometry,
        }

        # Сохраняем в файл
        safe_filename = cad_number.replace(":", "_") + ".geojson"
        file_path = output_folder / safe_filename

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(full_feature, f, ensure_ascii=False, indent=2,
                      default=json_serializer)

        return {
            "cad_number": cad_number,
            "status": "success",
            "file": str(file_path),
            "geometry_type": geojson_geometry.get("type", "Unknown"),
        }

    except asyncio.CancelledError:
        # Прокидываем дальше — нас останавливают снаружи
        raise
    except Exception as e:
        return {"cad_number": cad_number, "status": "error", "error": str(e)}


async def worker(nspd, cad_number: str, output_folder: Path,
                 sem: asyncio.Semaphore) -> dict:
    """Один воркер: ограничивает параллелизм и добавляет паузу."""
    async with sem:
        result = await get_geojson_for_one(nspd, cad_number, output_folder)
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
        return result


async def process_all(nspd, numbers: list[str], output_folder: Path) -> list[dict]:
    """
    Запускает все задачи параллельно с ограничением через Semaphore.
    Возвращает список результатов в исходном порядке.
    """
    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    tasks = [asyncio.create_task(worker(nspd, cn, output_folder, sem))
             for cn in numbers]

    results: list[dict] = []
    total = len(tasks)
    done = 0

    completed = False
    try:
        # as_completed позволяет печатать прогресс по мере завершения
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            done += 1

            status_icon = {
                "success": "✅",
                "not_found": "❌ не найден",
                "no_geometry": "📭 без геометрии",
                "error": "⚠️ ошибка",
            }.get(result["status"], "?")

            print(f"{format_progress(done, total)}  {result['cad_number']}  {status_icon}")

            # Инкрементальное сохранение отчёта
            if done % SAVE_EVERY == 0:
                save_report(output_folder, results, total, partial=True)
        completed = True
    finally:
        # При прерывании сохраняем то, что успели обработать
        if not completed and results:
            save_report(output_folder, results, total, partial=True)

    return results


def save_report(output_folder: Path, results: list[dict],
                total: int, partial: bool = False):
    """Сохраняет отчёт. При partial=True — помечает как промежуточный."""
    report_path = output_folder / "report.json"

    successful = sum(1 for r in results if r["status"] == "success")
    not_found = sum(1 for r in results if r["status"] == "not_found")
    no_geometry = sum(1 for r in results if r["status"] == "no_geometry")
    errors = sum(1 for r in results if r["status"] == "error")

    payload = {
        "partial": partial,
        "processed": len(results),
        "total": total,
        "successful": successful,
        "not_found": not_found,
        "no_geometry": no_geometry,
        "errors": errors,
        "results": results,
    }

    try:
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2,
                      default=json_serializer)
    except Exception as e:
        print(f"   ⚠️ Не удалось сохранить отчёт: {e}")


async def create_combined_geojson(output_folder: Path,
                                  successful_paths: list[str]) -> Path | None:
    """Создаёт один общий GeoJSON файл со всеми участками."""
    if not successful_paths:
        return None

    combined = {"type": "FeatureCollection", "features": []}

    for file_path in successful_paths:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                combined["features"].append(json.load(f))
        except Exception as e:
            print(f"   ⚠️ Не удалось добавить {file_path}: {e}")

    combined_path = output_folder / "all_parcels.geojson"
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2,
                  default=json_serializer)

    return combined_path


async def main():
    # Пути относительно папки скрипта
    script_dir = Path(__file__).resolve().parent
    input_file = script_dir / INPUT_FILE
    output_folder = script_dir / OUTPUT_FOLDER

    output_folder.mkdir(parents=True, exist_ok=True)

    # Читаем номера
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            cad_numbers = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ Файл {input_file} не найден!")
        return

    # Уникальные с сохранением порядка
    seen = set()
    unique_numbers = [n for n in cad_numbers
                      if not (n in seen or seen.add(n))]

    print(f"📋 Всего строк: {len(cad_numbers)}, уникальных: {len(unique_numbers)}")
    print(f"📁 Результаты: {output_folder}")
    print(f"⚙️  Параллельных запросов: {CONCURRENT_LIMIT}, "
          f"пауза между: {DELAY_BETWEEN_REQUESTS} сек")
    print("🚀 Старт...\n")

    async with AsyncNspd() as nspd:
        try:
            results = await process_all(nspd, unique_numbers, output_folder)
        except asyncio.CancelledError:
            print("\n\n⏹️  Прервано. Частичный отчёт сохранён в geojson_results/report.json")
            raise
        except KeyboardInterrupt:
            print("\n\n⏹️  Прервано пользователем.")
            raise

    # Финальный отчёт
    save_report(output_folder, results, len(unique_numbers), partial=False)

    successful = [r for r in results if r["status"] == "success"]
    successful_paths = [r["file"] for r in successful]

    combined_path = await create_combined_geojson(output_folder, successful_paths)

    not_found = sum(1 for r in results if r["status"] == "not_found")
    no_geometry = sum(1 for r in results if r["status"] == "no_geometry")
    errors = sum(1 for r in results if r["status"] == "error")

    print("\n" + "=" * 60)
    print("✅ ГОТОВО")
    print(f"   Всего:              {len(unique_numbers)}")
    print(f"   ✅ Успешно:          {len(successful)}")
    print(f"   ❌ Не найдено:       {not_found}")
    print(f"   📭 Без геометрии:    {no_geometry}")
    print(f"   ⚠️ Ошибок:           {errors}")
    print(f"   📁 Папка:            {output_folder}")

    if combined_path:
        size_mb = combined_path.stat().st_size / 1024 / 1024
        print(f"\n🗺️  Общий файл: {combined_path}")
        print(f"   Размер: {size_mb:.1f} МБ")
        if size_mb > SIZE_WARNING_MB:
            print(f"   ⚠️  Файл больше {SIZE_WARNING_MB} МБ — "
                  f"geojson.io может не открыть его целиком.")
            print(f"   💡 Откройте отдельные .geojson из папки {OUTPUT_FOLDER}/")
        print("\n💡 Как открыть:")
        print("   1. https://geojson.io")
        print("   2. Open → File, выберите all_parcels.geojson")
        print("   3. Все участки появятся на карте с границами")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Остановлено пользователем.")
        print("💡 Частичный отчёт сохранён в geojson_results/report.json "
              "(processed < total).")