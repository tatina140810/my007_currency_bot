import httpx
from app.core.logger import logger

# Webhook URL obtained from n8n instance
N8N_WEBHOOK_URL = "https://n8n.automationnodes.online/webhook/telegram-finance"

async def send_to_n8n(operation_data: dict):
    """
    Asynchronously sends operation data to the n8n webhook.
    
    Args:
        operation_data (dict): Dictionary containing details of the operation
            (e.g., chat_id, type, currency, amount, description, timestamp)
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                N8N_WEBHOOK_URL,
                json=operation_data,
                timeout=10.0  # 10 second timeout
            )
            response.raise_for_status()
            logger.info(f"[N8N] Successfully sent operation to n8n: {operation_data}")
            return True
            
    except httpx.HTTPStatusError as e:
        logger.error(f"[N8N] HTTP error when sending to n8n: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        logger.error(f"[N8N] Request error when sending to n8n: {e}")
    except Exception as e:
        logger.error(f"[N8N] Unexpected error sending to n8n: {e}")
        
    return False
