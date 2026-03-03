import logging
import json
from openai import AsyncOpenAI
from app.core.config import OPENAI_API_KEY

logger = logging.getLogger(__name__)

# Initialize client globally but only valid if key exists
client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

async def parse_swift_document(text: str) -> dict | None:
    """
    Sends OCR text to OpenAI to extract SWIFT/ISO 20022 fields.
    Returns a dict with 'documents' list, or None if it's not a SWIFT document.
    """
    if not client:
        logger.warning("SWIFT AI Parser called but OPENAI_API_KEY is not set.")
        return None

    system_prompt = """
    You are a financial document parser specializing in SWIFT and ISO 20022 payment documents.
    The input is raw OCR text extracted from a scanned document. The text may contain heavy OCR errors, typos, misaligned columns, or missing characters.
    The document may span multiple pages. There may be multiple <Document> blocks.

    Your tasks:
    1. Determine if the document represents a payment order, SWIFT transfer, or ISO 20022 message. Look for keywords like SWIFT, PAYMENT, TRANSFER, DEBIT, CREDIT, AMOUNT, CURRENCY, BIC, UETR, etc. Even if spelled incorrectly, try to recognize them.
    2. Extract the following fields:
       - document_id: Message ID, PrintRef, Reference, or UETR. Look for any unique alphanumeric identifier.
       - sender_name: Name of the debtor or sender (Dbtr/Nm or Ordering Customer).
       - sender_country: Country of the sender (Dbtr/PstlAdr/Ctry).
       - amount: The transaction amount (IntrBkSttlmAmt or equivalent).
       - currency: The transaction currency (e.g., USD, EUR, RUB, KZT).
       - uetr: The 36-character UETR code if present.
       - payment_for: Remittance Information, Details of Payment (RmtInf or Ustrd field).
    3. If you find data resembling a transfer, return an array of objects in the `documents` list.
    4. Return ONLY valid JSON. If a field cannot be found, return null. Do not hallucinate data that isn't roughly supported by the text.
    
    IMPORTANT FILTRATION RULE:
    Only reject the document (returning an empty array `{"documents": []}`) if it is absolutely clearly NOT a financial document (e.g., a photo of a cat, a random chat screenshot without numbers/currencies, or a food menu). If it looks like a bank statement, receipt, or SWIFT transfer even with heavy OCR damage, ATTEMPT TO EXTRACT the amount, currency, and sender.
    
    Output format:
    {
      "documents": [
        {
          "document_id": "...",
          "sender_name": "...",
          "sender_country": "...",
          "amount": "...",
          "currency": "...",
          "uetr": "...",
          "payment_for": "..."
        }
      ]
    }
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
            
        parsed_json = json.loads(result_content)
        docs = parsed_json.get("documents", [])
        
        if not docs:
            logger.info("AI determined this is not a SWIFT document.")
            return None
            
        valid_docs = []
        for doc in docs:
            # Basic validation to ensure it extracted *something*
            if doc.get("amount") and doc.get("currency"):
                # Clean amount
                try:
                    amount_str = str(doc["amount"]).replace(' ', '').replace(',', '.')
                    doc["amount"] = abs(float(amount_str))
                    doc["currency"] = str(doc["currency"]).upper()
                    valid_docs.append(doc)
                except ValueError:
                    logger.warning(f"Failed to parse amount from SWIFT AI: {doc['amount']}")
            
        return {"documents": valid_docs} if valid_docs else None

    except Exception as e:
        logger.error(f"Error in parse_swift_document: {e}")
        return None
