import asyncio
import random
import logging
import openai

logger = logging.getLogger(__name__)

MODELS_VISION = ["gpt-4o", "gpt-4o-mini"]
MODELS_TEXT = ["gpt-4o-mini", "gpt-4o", "gpt-3.5-turbo"]

async def call_openai_with_retry(client, messages, response_format=None, is_vision=False):
    """
    Calls OpenAI Chat Completions API with:
    - Automatic model fallback
    - Exponential backoff
    - Jitter
    - Retries on 429 and 503 errors
    """
    models = MODELS_VISION if is_vision else MODELS_TEXT
    
    max_retries = 5
    base_delay = 1.0 # Base delay in seconds
    
    for attempt in range(max_retries):
        for model in models:
            try:
                response = await client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=0.0,
                    response_format=response_format
                )
                return response
            except (openai.RateLimitError, openai.APIStatusError) as e:
                # 429 (Rate Limit) or 503 (Service Unavailable)
                status_code = getattr(e, 'status_code', getattr(e, 'code', None))
                # Treat some errors as retryable
                if status_code in (429, 502, 503, 500) or isinstance(e, openai.RateLimitError):
                    logger.warning(f"OpenAI error {status_code} with model {model}. Attempt {attempt+1}/{max_retries}. Trying next model...")
                    continue # Try the next model immediately
                else:
                    logger.error(f"OpenAI non-retryable error {status_code}: {e}")
                    raise e
            except openai.APIConnectionError as e:
                logger.warning(f"OpenAI connection error with model {model}: {e}")
                continue # Network errors also deserve a retry
            except Exception as e:
                logger.error(f"OpenAI unexpected error: {e}")
                raise e
                
        # If ALL models fail in this attempt, do exponential backoff + jitter before the next attempt
        if attempt < max_retries - 1:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"All models failed on attempt {attempt+1}. Retrying in {delay:.2f} seconds...")
            await asyncio.sleep(delay)
            
    logger.error("Max retries reached for OpenAI call.")
    return None
