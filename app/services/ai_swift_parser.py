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
    You are a SWIFT payment document parser.
    The input is OCR text extracted from scanned SWIFT / ISO 20022 documents.
    The document may span multiple pages. There may be multiple <Document> blocks.

    Your tasks:
    1. Detect each complete SWIFT document.
    2. Extract the following fields:
       - document_id (MsgId or PrintRef)
       - sender_name (Dbtr/Nm)
       - sender_country (Dbtr/PstlAdr/Ctry if available)
       - amount (IntrBkSttlmAmt value)
       - currency (IntrBkSttlmAmt Ccy attribute)
       - uetr
       - payment_for (RmtInf or Ustrd field)
    3. If multiple documents exist, return an array of objects.
    4. Return ONLY valid JSON.
    5. If a field is missing, return null.
    6. Do not hallucinate.
    
    IMPORTANT FILTRATION RULE:
    If the text clearly does NOT contain any SWIFT, XML, or ISO 20022 payment data (e.g. it's a random photo, meme, or chat screenshot), you MUST return an empty array for documents: { "documents": [] }.
    
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
