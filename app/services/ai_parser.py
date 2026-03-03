import logging
import json
from openai import AsyncOpenAI
from app.core.config import OPENAI_API_KEY, CURRENCIES, OPERATION_TYPES

logger = logging.getLogger(__name__)

# Initialize client globally but only valid if key exists
client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

async def parse_with_ai(text: str) -> dict | None:
    """
    Sends unstructured text to OpenAI to parse into a structured financial operation.
    Returns a dict with: type, currency, amount, description, group (optional)
    """
    if not client:
        logger.warning("AI Parser called but OPENAI_API_KEY is not set.")
        return None

    system_prompt = f"""
    You are a financial data extraction assistant for a Telegram bot.
    Your job is to extract financial operation details from unstructured text messages.
    
    You MUST return ONLY a valid JSON object. Do not include markdown formatting or extra text.
    
    The JSON object MUST have these exact keys:
    - "type": MUST be one of {OPERATION_TYPES}
    - "currency": MUST be one of {CURRENCIES}
    - "amount": MUST be a positive float number (e.g., 6655.80)
    - "description": A short string summarizing the payment details (e.g., "Инвойс HXD0235 от 05.02").
    - "group": (Optional string, usually null)
    
    RULES:
    1. If the text says "оплата" or "инвойс" or "отправить", the type is usually "Оплата ПП" (Payment).
    2. If the text says "поступили" or "зачислено", the type is usually "Поступление" (Income).
    3. If the text says "выдача" or "наличные", the type is usually "Выдача наличных" (Cash withdrawal).
    4. If the text says "взнос", the type is usually "Взнос наличными" (Cash deposit).
    5. Convert currency names to their standard codes (e.g., "долларах" -> "USD", "евро" -> "EUR", "руб" -> "RUB").
    6. ALWAYS return amount as a positive number. The bot handles signs based on the operation type.
    7. If you cannot confidently extract an amount and a currency, return an empty JSON object: {{}}
    
    EXAMPLES:
    Input: "Сумма к оплате в долларах 6655,80. Номер Инвойса HXD0235 от 05.02.2026"
    Output: {{"type": "Оплата ПП", "currency": "USD", "amount": 6655.80, "description": "Номер Инвойса HXD0235 от 05.02.2026", "group": null}}
    
    Input: "Оплатите 120 000 руб за доставку"
    Output: {{"type": "Оплата ПП", "currency": "RUB", "amount": 120000.0, "description": "за доставку", "group": null}}
    """

    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text}
            ],
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        
        result_content = response.choices[0].message.content.strip()
        
        if not result_content or result_content == "{}":
            return None
            
        parsed_data = json.loads(result_content)
        
        # Validation
        req_keys = ["type", "currency", "amount"]
        if not all(k in parsed_data for k in req_keys):
            logger.warning(f"AI missing keys: {parsed_data}")
            return None
            
        if parsed_data["type"] not in OPERATION_TYPES:
            logger.warning(f"AI invalid type: {parsed_data['type']}")
            return None
            
        if parsed_data["currency"] not in CURRENCIES:
            logger.warning(f"AI invalid currency: {parsed_data['currency']}")
            return None
            
        # Ensure positive float
        parsed_data["amount"] = abs(float(parsed_data["amount"]))
        
        if "description" not in parsed_data:
            parsed_data["description"] = "Parsed via AI"
            
        return parsed_data

    except Exception as e:
        logger.error(f"Error in parse_with_ai: {e}")
        return None
