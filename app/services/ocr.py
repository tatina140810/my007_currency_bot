"""
Улучшенный OCR для финансовых документов (SWIFT)
Использует продвинутую предобработку изображений и множественные попытки распознавания
"""

import io
import re
import logging
import numpy as np
from PIL import Image, ImageEnhance, ImageFilter

logger = logging.getLogger(__name__)

# Попытка импорта OpenCV
try:
    import cv2
    HAS_CV2 = True
    logger.info("✅ OpenCV доступен")
except ImportError:
    HAS_CV2 = False
    logger.warning("⚠️ OpenCV не установлен, используется базовая предобработка")

# Попытка импорта EasyOCR
try:
    import easyocr
    HAS_EASYOCR = True
    _easyocr_reader = None
    logger.info("✅ EasyOCR доступен")
except ImportError:
    HAS_EASYOCR = False
    logger.warning("⚠️ EasyOCR не установлен, используется Tesseract")


def preprocess_image_basic(image_bytes: bytes) -> Image:
    """
    Базовая предобработка (без OpenCV)
    """
    img = Image.open(io.BytesIO(image_bytes))

    # Конвертация в grayscale
    img = img.convert('L')

    # Увеличиваем контраст
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(2.5)

    # Увеличиваем резкость
    img = img.filter(ImageFilter.SHARPEN)
    img = img.filter(ImageFilter.EDGE_ENHANCE)

    # Увеличиваем изображение если маленькое
    w, h = img.size
    if w < 2000:
        scale = 2000 / w
        new_w = int(w * scale)
        new_h = int(h * scale)
        img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
        logger.info(f"    Изображение увеличено до {new_w}x{new_h}")

    return img


def preprocess_image_fast(image_bytes: bytes) -> Image:
    """
    БЫСТРАЯ предобработка БЕЗ тяжёлых операций OpenCV
    Для слабых серверов
    """
    img = Image.open(io.BytesIO(image_bytes))

    # 1. Grayscale
    img = img.convert('L')

    # 2. Увеличиваем контраст (PIL - быстро!)
    from PIL import ImageEnhance
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(2.5)

    # 3. Оптимальный размер: 1400px (меньше = быстрее)
    width, height = img.size
    if width < 1400:
        scale = 1400 / width
        new_width = int(width * scale)
        new_height = int(height * scale)
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        logger.info(f"    Изображение увеличено до {new_width}x{new_height}")
    elif width > 1600:
        scale = 1600 / width
        new_width = int(width * scale)
        new_height = int(height * scale)
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        logger.info(f"    Изображение уменьшено до {new_width}x{new_height}")

    return img


def preprocess_image_advanced(image_bytes: bytes) -> Image:
    """
    Продвинутая предобработка с OpenCV (для мощных серверов)
    """
    img = Image.open(io.BytesIO(image_bytes))

    # 1. Конвертация в grayscale
    img = img.convert('L')

    # 2. Конвертируем в numpy для OpenCV
    img_array = np.array(img)

    # 3. Оптимальный размер: 1400px (уменьшен ещё больше!)
    height, width = img_array.shape
    target_width = 1400  # Было 1800, теперь 1400

    if width < target_width:
        scale = target_width / width
        new_width = int(width * scale)
        new_height = int(height * scale)
        img_array = cv2.resize(
            img_array, (new_width, new_height),
            interpolation=cv2.INTER_LINEAR  # БЫСТРЕЕ чем CUBIC!
        )
        logger.info(f"    Изображение увеличено до {new_width}x{new_height}")
    elif width > 1600:
        scale = 1600 / width
        new_width = int(width * scale)
        new_height = int(height * scale)
        img_array = cv2.resize(
            img_array, (new_width, new_height),
            interpolation=cv2.INTER_AREA
        )
        logger.info(f"    Изображение уменьшено до {new_width}x{new_height}")

    # 4. ПРОПУСКАЕМ fastNlMeansDenoising - СЛИШКОМ МЕДЛЕННО!

    # 5. Упрощённая улучшение контраста
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(4,4))  # Меньше параметры
    img_array = clahe.apply(img_array)

    # 6. Простая бинаризация Otsu (быстрее адаптивной!)
    _, img_array = cv2.threshold(
        img_array, 0, 255,
        cv2.THRESH_BINARY + cv2.THRESH_OTSU
    )

    return Image.fromarray(img_array)


def run_tesseract_ocr(img: Image) -> str:
    """
    Запускает Tesseract OCR - СУПЕР БЫСТРЫЙ режим
    """
    import pytesseract

    # ТОЛЬКО ОДНА ПОПЫТКА - максимальная скорость!
    try:
        logger.info(f"    OCR: Tesseract FAST (timeout 90с)...")

        text = pytesseract.image_to_string(
            img,
            lang='eng',
            config='--oem 1 --psm 6',  # Самый быстрый режим
            timeout=90  # Один большой timeout
        ) or ""

        logger.info(f"    OCR: результат → {len(text)} символов")

        return text

    except RuntimeError:
        logger.error(f"    OCR: TIMEOUT (90с)")
        return ""
    except Exception as e:
        logger.error(f"    OCR: ошибка - {e}")
        return ""


def run_easyocr(image_bytes: bytes) -> str:
    """
    EasyOCR - более точный для финансовых документов
    """
    global _easyocr_reader

    if not HAS_EASYOCR:
        logger.warning("    EasyOCR не установлен!")
        return ""

    try:
        # Инициализируем reader один раз
        if _easyocr_reader is None:
            logger.info("    Инициализация EasyOCR...")
            _easyocr_reader = easyocr.Reader(['en'], gpu=False)
            logger.info("    EasyOCR готов")

        # Предобработка
        if HAS_CV2:
            img = preprocess_image_advanced(image_bytes)
        else:
            img = preprocess_image_basic(image_bytes)

        img_array = np.array(img)

        # OCR
        logger.info("    Запуск EasyOCR...")
        results = _easyocr_reader.readtext(
            img_array,
            detail=1,  # Возвращает координаты + уверенность
            paragraph=False,
            batch_size=4
        )

        # Собираем текст
        text_lines = []
        for (bbox, text, confidence) in results:
            if confidence > 0.2:  # Фильтруем низкую уверенность
                text_lines.append(text)
                logger.debug(f"      {text} (confidence: {confidence:.2f})")

        full_text = '\n'.join(text_lines)
        logger.info(f"    EasyOCR: {len(full_text)} символов")

        return full_text

    except Exception as e:
        logger.exception("    EasyOCR ошибка")
        return ""


def run_ocr_from_image_bytes(image_bytes: bytes, use_easyocr: bool = True) -> str:
    """
    Главная функция OCR - автоматически выбирает лучший метод

    Args:
        image_bytes: Байты изображения
        use_easyocr: Попытаться использовать EasyOCR (если установлен)

    Returns:
        Распознанный текст
    """
    import pytesseract

    try:
        w, h = Image.open(io.BytesIO(image_bytes)).size
        pixels = w * h
        logger.info(f"    OCR: {w}x{h} ({pixels/1_000_000:.1f} Мпикс)")

        # Пробуем EasyOCR если доступен и запрошен
        if use_easyocr and HAS_EASYOCR:
            logger.info("    Использую EasyOCR (точнее для цифр)")
            text = run_easyocr(image_bytes)
            if text:
                return text.strip()
            logger.warning("    EasyOCR не дал результатов, переключаюсь на Tesseract")

        # Предобработка изображения - БЫСТРАЯ версия
        if HAS_CV2:
            logger.info("    Быстрая предобработка (без тяжёлых операций)")
            img = preprocess_image_fast(image_bytes)
        else:
            logger.info("    Базовая предобработка (без OpenCV)")
            img = preprocess_image_basic(image_bytes)

        # Запускаем Tesseract
        logger.info("    Использую Tesseract OCR")
        text = run_tesseract_ocr(img)

        if not text:
            logger.error("    OCR не дал результатов!")
            return ""

        # Нормализация символов
        text = text.replace("‹", "<").replace("›", ">")
        text = text.replace("«", "<").replace("»", ">")

        logger.info(f"    OCR завершён: {len(text)} символов")

        return text.strip()

    except Exception as e:
        logger.exception("    OCR критическая ошибка")
        return ""


def extract_amount_from_swift(text: str) -> dict | None:
    """
    Извлекает суммы из SWIFT с множественной проверкой

    Returns:
        dict с keys: currency, amount, raw
        или None если не найдено
    """
    if not text:
        return None

    patterns = [
        # <InstdAmt Ccy="EUR">118028.80</InstdAmt>
        r'<InstdAmt\s+Ccy="([A-Z]{3})">(\d+(?:[.,]\d+)?)</InstdAmt>',

        # <IntrBkSttlmAmt Ccy="EUR">118028.80</IntrBkSttlmAmt>
        r'<IntrBkSttlmAmt\s+Ccy="([A-Z]{3})">(\d+(?:[.,]\d+)?)</IntrBkSttlmAmt>',

        # Прямое указание с валютой
        r'Ccy="([A-Z]{3})">(\d+(?:[.,]\d+)?)',

        # EUR>118028.80 (без кавычек)
        r'([A-Z]{3})>(\d{1,3}(?:[,.\s]\d{3})*(?:[.,]\d{1,2})?)',

        # EUR 118028.80 (пробел)
        r'([A-Z]{3})\s+(\d{1,3}(?:[,.\s]\d{3})*(?:[.,]\d{1,2})?)',
    ]

    found_amounts = []

    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            currency = match.group(1).upper()
            amount_str = match.group(2)

            # Нормализуем сумму
            amount_str = amount_str.replace(' ', '').replace(',', '.')

            try:
                amount = float(amount_str)

                # Валидация: суммы обычно от 0.01 до 10,000,000
                if 0.01 <= amount <= 10_000_000:
                    found_amounts.append({
                        'currency': currency,
                        'amount': amount,
                        'raw': amount_str,
                        'pattern': pattern
                    })
                    logger.info(f"      Найдена сумма: {amount} {currency}")

            except ValueError:
                continue

    if not found_amounts:
        logger.warning("    Суммы не найдены в тексте")
        return None

    # Выбираем самую большую сумму (обычно это основная)
    best = max(found_amounts, key=lambda x: x['amount'])
    logger.info(f"    ✅ ИТОГОВАЯ СУММА: {best['amount']} {best['currency']}")

    return best


def get_ocr_capabilities() -> dict:
    """
    Возвращает информацию о доступных возможностях OCR
    """
    return {
        'opencv': HAS_CV2,
        'easyocr': HAS_EASYOCR,
        'tesseract': True  # предполагаем что всегда есть
    }
