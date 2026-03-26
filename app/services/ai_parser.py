import logging
import json
from openai import AsyncOpenAI
from app.core.config import OPENAI_API_KEY, CURRENCIES, OPERATION_TYPES
from app.services.ai_retry import call_openai_with_retry

logger = logging.getLogger(__name__)

# Initialize client globally but only valid if key exists
# client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

async def parse_with_ai(text: str, reply_context: str = None) -> list[dict] | None:
    """
    Sends unstructured text to OpenAI to parse into structured financial operations.
    Returns a list of dicts with: type, currency, amount, description, group (optional)
    """
    from app.core.config import OPENAI_API_KEY
    client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

    if not client:
        logger.warning("AI Parser called but OPENAI_API_KEY is not set.")
        return None

    # Fetch available group names from DB to help AI route messages without tags
    from app.db.instance import db
    all_chats = db.get_all_chats()
    group_names = [chat[1] for chat in all_chats if chat[2] in ("group", "supergroup", "virtual_group") and chat[1]]
    group_names_str = ", ".join(f'"{g}"' for g in group_names)

    system_prompt = f"""
    You are an expert financial assistant for a Telegram bot tracking cash flows.
    Your job is to extract financial operations from unstructured user messages and return them as a strict JSON ARRAY inside an object.
    
    You MUST return ONLY a valid JSON object with a single key "operations", which contains a list of operations.
    Example: {{ "operations": [ {{...}}, {{...}} ] }}
    
    Each operation object MUST have these exact keys:
    - "type": MUST be one of {OPERATION_TYPES} or "Internal Exchange"
    - "currency": MUST be one of {CURRENCIES}
    - "amount": MUST be a positive float number (e.g., 6655.80)
    - "description": A short string summarizing the payment details (e.g., "Инвойс HXD0235").
    - "group": (Optional string). If the text mentions or implies a client or group, you MUST return one of these exact group names: [{group_names_str}]. If no group is mentioned and it's ambiguous, return null.
    
    RULES:
    1. "Оплата ПП" (Payment): "оплата", "инвойс", "отправить".
    2. "Поступление" (Income): "поступили", "зачислено".
    3. "Выдача наличных" (Cash withdrawal): "выдача", "наличные", "выдали".
    4. "Взнос наличными" (Cash deposit): "взнос".
    5. Currencies: Convert to standard codes ("долларах" -> "USD", "евро" -> "EUR", "руб" -> "RUB").
    6. Amounts: ALWAYS positive numbers. The bot handles mathematical signs.
    7. Ignore pure balance/residual statements (e.g., "Ост 1979471₽", "остаток 100").
    8. CRITICAL — IGNORE INFORMATIONAL MESSAGES. Return {{ "operations": [] }} if the message:
       - Mentions a date of account opening, document date, or any date reference without a financial amount (e.g. "дата открытия счета ... - 21.07.2025").
       - Is a clarifying question or administrative notice without a monetary amount.
       - Contains phrases like "дата открытия", "дата счета", "можем указать", "какую дату укажем", "добрый день" without a monetary transaction.
       A date alone is NEVER a financial operation.
    
    ADVANCED SCENARIOS (Phase 2):
    A. **Currency Exchange (Выкуп)**: 
       If a user says "6655,80 долларов на выкуп" and mentions a rate (e.g., "79.80"), it means they bought 6655.80 USD by paying RUB (6655.80 * 79.80 = 531132.84 RUB).
       You MUST return TWO operations:
       1) {{"type": "Internal Exchange", "currency": "USD", "amount": 6655.80, "description": "Покупка USD по курсу 79.80", "group": <matched_group_if_any>}}
       2) {{"type": "Internal Exchange", "currency": "RUB", "amount": 531132.84, "description": "Оплата за USD по курсу 79.80", "group": <matched_group_if_any>}}
    
    B. **Commission (Комиссия)**:
       If the user says "комиссия банка 150$", return:
       {{"type": "Комиссия 1%", "currency": "USD", "amount": 150.0, "description": "Комиссия банка", "group": <matched_group_if_any>}}
       
    C. **Empty state**: If you cannot confidently extract an amount and currency, return {{ "operations": [] }}.
    """

    # Fetch dynamic training examples (Human-in-the-loop)
    training_examples = db.get_ai_training_examples(limit=5)
    
    if training_examples:
        system_prompt += "\n\nCRITICAL: LEARN FROM THESE PREVIOUS CORRECTIONS BY THE ADMIN:\n"
        for ex in training_examples:
            system_prompt += (
                f"User said: '{ex['original_text']}'\n"
                f"CORRECT OUTPUT MUST EXTRACT: Type: {ex['op_type']}, Amount: {ex['amount']}, Currency: {ex['currency']}\n\n"
            )

    user_content = text
    if reply_context:
        user_content = f"CONTEXT (Original Message being replied to):\n'{reply_context}'\n\nUSER MESSAGE:\n'{text}'"

    try:
        response = await call_openai_with_retry(
            client=client,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            response_format={"type": "json_object"},
            is_vision=False
        )
        
        if not response:
            return None
        
        result_content = response.choices[0].message.content.strip()
        
        if not result_content or result_content == "{}":
            return None
            
        parsed_json = json.loads(result_content)
        ops = parsed_json.get("operations", [])
        
        valid_ops = []
        for parsed_data in ops:
            # Validation
            req_keys = ["type", "currency", "amount"]
            if not all(k in parsed_data for k in req_keys):
                logger.warning(f"AI missing keys: {parsed_data}")
                continue
                
            if parsed_data["type"] not in OPERATION_TYPES and parsed_data["type"] != "Internal Exchange":
                logger.warning(f"AI invalid type: {parsed_data['type']}")
                continue
                
            if parsed_data["currency"] not in CURRENCIES:
                logger.warning(f"AI invalid currency: {parsed_data['currency']}")
                continue
                
            # Ensure positive float
            parsed_data["amount"] = abs(float(parsed_data["amount"]))
            
            if "description" not in parsed_data:
                parsed_data["description"] = "Parsed via AI"
                
            valid_ops.append(parsed_data)
            
        return valid_ops if valid_ops else None

    except Exception as e:
        logger.error(f"Error in parse_with_ai: {e}")
        return None
