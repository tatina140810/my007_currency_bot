# photo_handler_optimized.py
# ОПТИМИЗИРОВАННАЯ ОБРАБОТКА SWIFT ФОТОГРАФИЙ

import io
import logging
import asyncio
from PIL import Image, ImageOps, ImageFilter, ImageEnhance
import pytesseract
import re

logger = logging.getLogger(__name__)


def preprocess_image_for_ocr(image_bytes: bytes, quick: bool = False) -> Image.Image:
    """
    Предобработка изображения для OCR.

    Args:
        image_bytes: Байты изображения
        quick: Если True - быстрая обработка для предпроверки

    Returns:
        Обработанное изображение PIL
    """
    img = Image.open(io.BytesIO(image_bytes))

    # 1) Конвертация в градации серого
    img = img.convert("L")

    # 2) Автоконтраст (улучшает читаемость)
    img = ImageOps.autocontrast(img, cutoff=2)

    # 3) Увеличение контрастности
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(1.5)

    if quick:
        # Для быстрой проверки - минимальная обработка
        return img

    # 4) Резкость (только для полного OCR)
    img = img.filter(ImageFilter.UnsharpMask(radius=1.5, percent=150, threshold=3))

    # 5) Умеренный upscale (1.5x вместо 2x для скорости)
    w, h = img.size
    img = img.resize((int(w * 1.5), int(h * 1.5)), Image.Resampling.LANCZOS)

    return img


def quick_ocr_check(image_bytes: bytes) -> tuple[bool, str]:
    """
    Быстрая проверка - это SWIFT документ или нет?
    Делает упрощённый OCR только для поиска ключевых маркеров.

    Returns:
        (is_swift, text_sample) - найдены ли SWIFT маркеры и образец текста
    """
    try:
        # Быстрая предобработка
        img = preprocess_image_for_ocr(image_bytes, quick=True)

        # Быстрый OCR с самым простым режимом
        text = pytesseract.image_to_string(
            img,
            lang="eng",
            config="--oem 3 --psm 6"  # Быстрый режим
        )

        # Нормализация
        text = text.replace("‹", "<").replace("›", ">")
        text = text.replace("«", "<").replace("»", ">")

        # Ищем явные маркеры SWIFT
        low = text.lower()
        swift_markers = [
            "pacs.008",
            "iso20022",
            "<uetr>",
            "swiftnet",
            "printerbakay",
            "bakakg22",
        ]

        is_swift = any(marker in low for marker in swift_markers)

        # Или несколько XML тегов
        if not is_swift:
            xml_tags = ["<document", "<fito", "<intrbk", "<bicfi", "<pmtid"]
            is_swift = sum(1 for tag in xml_tags if tag in low) >= 2

        logger.info(f"🔍 Быстрая проверка: {'✅ SWIFT' if is_swift else '❌ не SWIFT'}")
        return is_swift, text

    except Exception as e:
        logger.error(f"❌ Ошибка быстрой проверки: {e}")
        return False, ""


def full_ocr(image_bytes: bytes) -> str:
    """
    Полный OCR с оптимальной конфигурацией.
    Используется только если quick_ocr_check подтвердил SWIFT.
    """
    try:
        # Полная предобработка
        img = preprocess_image_for_ocr(image_bytes, quick=False)

        # Один проход с оптимальной конфигурацией
        # psm 6 = единый блок текста (подходит для SWIFT документов)
        text = pytesseract.image_to_string(
            img,
            lang="eng",
            config="--oem 3 --psm 6 -c preserve_interword_spaces=1"
        )

        # Нормализация кавычек и скобок
        text = text.replace("‹", "<").replace("›", ">")
        text = text.replace("«", "<").replace("»", ">")

        logger.info(f"✅ Полный OCR завершён: {len(text)} символов")
        return text.strip()

    except Exception as e:
        logger.error(f"❌ Ошибка полного OCR: {e}")
        return ""


async def process_swift_photo(
    image_bytes: bytes,
    use_quick_check: bool = True
) -> tuple[bool, str]:
    """
    Обработка фото SWIFT документа.

    Args:
        image_bytes: Байты изображения
        use_quick_check: Использовать быструю предпроверку

    Returns:
        (is_swift, ocr_text) - это SWIFT и полный текст OCR
    """
    logger.info("📸 Начинаю обработку фото")

    # Шаг 1: Быстрая проверка (опционально)
    if use_quick_check:
        is_swift, quick_text = await asyncio.to_thread(quick_ocr_check, image_bytes)

        if not is_swift:
            logger.info("⏭️ Фото не похоже на SWIFT, пропускаю полный OCR")
            return False, quick_text

        logger.info("✅ Фото похоже на SWIFT, запускаю полный OCR")

    # Шаг 2: Полный OCR (только если это SWIFT)
    full_text = await asyncio.to_thread(full_ocr, image_bytes)

    return True, full_text


async def process_multiple_photos(
    photos_bytes: list[bytes]
) -> tuple[bool, str]:
    """
    Обработка нескольких фото (многостраничный SWIFT).

    Args:
        photos_bytes: Список байтов изображений

    Returns:
        (is_swift, combined_text) - это SWIFT и объединённый текст
    """
    logger.info(f"📸 Обрабатываю {len(photos_bytes)} фото")

    # Обрабатываем фото параллельно
    tasks = [
        process_swift_photo(photo_bytes, use_quick_check=True)
        for photo_bytes in photos_bytes
    ]

    results = await asyncio.gather(*tasks)

    # Фильтруем только SWIFT страницы
    swift_texts = [text for is_swift, text in results if is_swift and text]

    if not swift_texts:
        logger.info("⛔ Ни одна страница не является SWIFT документом")
        return False, ""

    # Объединяем текст всех страниц
    combined_text = "\n\n--- NEXT PAGE ---\n\n".join(swift_texts)

    logger.info(f"✅ Обработано {len(swift_texts)} страниц SWIFT, всего {len(combined_text)} символов")
    return True, combined_text
