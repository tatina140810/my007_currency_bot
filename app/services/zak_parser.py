import re
from app.services.parser import parse_human_number, normalize_currency

def _find_fee_mode(text: str) -> str:
    """Returns 'included' or 'extra' based on whether the fee is included in the amount."""
    if "взнос" in text.lower() and "включен" in text.lower():
        return "included"
    return "extra"

def _find_percent(text: str):
    """Parses percentage string and its numeric value (e.g., '0,1%' -> '0.1%', 0.001)"""
    m = re.search(r'([\d.,]+)\s*%', text)
    if m:
        val_str = m.group(1).replace(',', '.')
        try:
            val = float(val_str) / 100.0
            return f"{val_str}%", val
        except ValueError:
            pass
    return None, 0.0

def _find_currency(text: str) -> str | None:
    """Extracts currency from a line or header."""
    curr_match = re.search(r'(?i)(руб|р\b|сом|kgs|usd|\$|долл|евро|eur|€|kzt|тенге|cny|юань|¥|руб\.)', text)
    if curr_match:
        raw = curr_match.group(1).lower()
        if raw in ['сом', 'сомы', 'kgs']:
            return 'KGS'
        if raw in ['р', 'р.', 'руб', 'руб.']:
            return 'RUB'
        return normalize_currency(raw)
    return None

def _parse_special_amount(amount_str: str) -> float:
    amount_str = amount_str.lower().strip()
    multiplier = 1.0
    if "млн" in amount_str:
        multiplier = 1000000.0
        amount_str = amount_str.replace("млн", "").strip()
    elif "тыс" in amount_str or "т.р" in amount_str or "т.с" in amount_str:
        multiplier = 1000.0
        amount_str = re.sub(r'т\.?[ррс].*', '', amount_str).strip()
        amount_str = amount_str.replace("тыс", "").strip()
        
    try:
        val = parse_human_number(amount_str)
        return val * multiplier
    except Exception:
        return 0.0

def parse_zak_message(text: str, chat_id: int, message_id: int, msg_date) -> list[dict]:
    """
    Parses a combined message from the 'Зак' group.
    Extracts individual fee-bearing operations and comments.
    """
    lines = text.strip().split('\n')
    
    results = []
    
    current_type = None
    current_bank = None
    current_currency = None
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        # Detect Header
        # "СНЯТИЯ РСК:", "ПОПОЛНЕНИЯ БАКАЙ: (долл)"
        header_match = re.match(r'(?i)(СНЯТИ[ЕЯ]\s+|ПОПОЛНЕНИ[ЕЯ]\s+)(РСК|БАКАЙ)(.*)', line)
        if header_match:
            op_str = header_match.group(1).strip().lower()
            if 'сняти' in op_str:
                current_type = 'Снятие'
            elif 'пополнени' in op_str:
                current_type = 'Пополнение'
                
            current_bank = header_match.group(2).upper()
            
            # Check for currency in header
            rest = header_match.group(3)
            curr = _find_currency(rest)
            if curr:
                current_currency = curr
            else:
                current_currency = None # Reset if not found
            continue

        # Look for operation lines like:
        # "интеко - 4 360 000р по 0,1%"
        # "Умут - 85 800 $ (взнос 0,1% включен в эту сумму)"
        
        # It usually has a company, a hyphen, an amount, and a percentage.
        # We can try to split by hyphen or look for amounts.
        # Format: Company - Amount Currency [optional text] % [optional text]
        
        op_match = re.match(r'^([^\-]+)\s*-\s*([\d\s.,]+)(.*?)$', line)
        
        # Разрешаем парсить операцию даже если нет % в строке, чтобы потом подхватить % со следующей строки
        if op_match:
            company = op_match.group(1).strip()
            amount_str = op_match.group(2).strip()
            rest = op_match.group(3).strip()
            
            try:
                amount = parse_human_number(amount_str)
            except Exception:
                amount = 0.0
                
            if amount > 0:
                line_curr = _find_currency(line)
                final_curr = line_curr if line_curr else current_currency
                if not final_curr:
                    final_curr = 'RUB'
                    
                percent_str, percent_val = _find_percent(line)
                fee_mode = _find_fee_mode(line)
                
                if fee_mode == 'extra':
                    fee = amount * percent_val
                    net_amount = amount
                    gross_amount = amount + fee
                else:
                    gross_amount = amount
                    fee = amount * percent_val / (1 + percent_val)
                    net_amount = amount - fee
                    
                results.append({
                    "date": msg_date,
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "type": current_type or "Unknown",
                    "bank": current_bank or "Unknown",
                    "company": company,
                    "currency": final_curr,
                    "amount": amount,
                    "percent_str": percent_str or "0%",
                    "percent_value": percent_val,
                    "fee_mode": fee_mode,
                    "fee": fee,
                    "net_amount": net_amount,
                    "gross_amount": gross_amount,
                    "comment": "",
                    "raw_line": line,
                    "raw_text": text
                })
                continue
                
        # Check for numeric cash withdrawal comments (anchored to start)
        cash_match = re.search(r'(?i)^[^\wа-яa-z]*(?:снимите|выдай(?:те)?)\s+нал(?:ичные)?(?:[\sа-я]*в\s+размере)?\s+([\d\s.,]+(?:млн|тыс|т\.?р|k)?)\s*([a-zа-я$€¥]+)?', line)
        if cash_match:
            amount_str = cash_match.group(1).strip()
            curr_str = cash_match.group(2)
            amount = _parse_special_amount(amount_str)
            if amount > 0:
                final_curr = _find_currency(curr_str or line) if curr_str or line else None
                if not final_curr:
                    final_curr = current_currency or 'KGS'
                
                # Check if it has a percentage somehow
                percent_str, percent_val = _find_percent(line)
                fee = amount * percent_val
                
                results.append({
                    "date": msg_date,
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "type": "Снятие",
                    "bank": current_bank or "Unknown",
                    "company": "Наличные",
                    "currency": final_curr,
                    "amount": amount,
                    "percent_str": percent_str or "0%",
                    "percent_value": percent_val,
                    "fee_mode": "extra",
                    "fee": fee,
                    "net_amount": amount,
                    "gross_amount": amount + fee,
                    "comment": line,
                    "raw_line": line,
                    "raw_text": text
                })
                continue
                
        # If it doesn't match an operation, it's a comment/tag
        # Check if the comment contains a percentage that we can retroactively attach to the previous operation
        percent_str, percent_val = _find_percent(line)
        if percent_val > 0 and len(results) > 0:
            last_op = results[-1]
            if last_op.get("amount", 0) > 0 and last_op.get("percent_value", 0) == 0:
                # Upgrade the last operation
                last_op["percent_str"] = percent_str
                last_op["percent_value"] = percent_val
                last_op["comment"] = (last_op.get("comment", "") + " | " + line).strip(" | ")
                
                # Recalculate fee architecture based on new percentage binding
                fee_mode = _find_fee_mode(line)
                last_op["fee_mode"] = fee_mode
                amount = last_op["amount"]
                
                if fee_mode == 'extra':
                    fee = amount * percent_val
                    net_amount = amount
                    gross_amount = amount + fee
                else:
                    gross_amount = amount
                    fee = amount * percent_val / (1 + percent_val)
                    net_amount = amount - fee
                    
                last_op["fee"] = fee
                last_op["net_amount"] = net_amount
                last_op["gross_amount"] = gross_amount
                continue

        results.append({
            "date": msg_date,
            "chat_id": chat_id,
            "message_id": message_id,
            "type": current_type or "Unknown",
            "bank": current_bank or "Unknown",
            "company": "",
            "currency": "",
            "amount": 0.0,
            "percent_str": "",
            "percent_value": 0.0,
            "fee_mode": "",
            "fee": 0.0,
            "net_amount": 0.0,
            "gross_amount": 0.0,
            "comment": line,
            "raw_line": line,
            "raw_text": text,
            "comment_only": True
        })

    return results
