import re
from typing import Optional, List, Dict

from app.services.parser import parse_human_number, normalize_currency
from app.core.config import CURRENCIES

def parse_group_conversions(text: str, msg_id: Optional[int] = None) -> List[Dict]:
    """
    Парсит конвертации из группы "Курсы, конветации, суммы".
    Ожидаемый формат строк: "236000 ю 11.73 экспо"
    (Количество, Валюта, Курс, Клиент)
    
    ПОЖЕЛАНИЕ ПОЛЬЗОВАТЕЛЯ: "Этот код не должен подвергаться изменениям в случае изменения другой логики"
    """
    if not text:
        return []
        
    results = []
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        # Паттерн: Число (с пробелами/точками) + Валюта (возможно слитно) + Число (с точкой, возможно слитно с валютой) + Текст (клиент)
        # Пример: 236000 ю 11.73 экспо, 3000ев 93.50 ворд, 180 000 ю11.95 фининфра
        match = re.match(r"^([\d\s.,]+)\s*([a-zA-Zа-яА-Я$€¥]{1,8})\s*([\d.,]+)\s+(.+)$", line)
        if match:
            amount_str = match.group(1).strip()
            # If the user typed 736.000ю, we treat the dot as a thousands separator.
            amount_str = amount_str.replace(" ", "")
            if re.search(r"\.\d{3}$", amount_str):
                amount_str = amount_str.replace(".", "")

            curr_str = match.group(2).strip()
            rate_str = match.group(3).strip()
            client_str = match.group(4).strip()
            
            try:
                amt = parse_human_number(amount_str)
                rate = parse_human_number(rate_str)
                
                # Защита от случайных сообщений
                if amt <= 0 or rate <= 0:
                    continue
                    
                curr = normalize_currency(curr_str)
                if curr not in CURRENCIES:
                    continue  # Игнорируем, если валюта не распознана системой как валидная
                    
                res_dict = {
                    "amount": amt,
                    "currency": curr,
                    "rate": rate,
                    "client": client_str,
                    "original_text": line
                }
                if msg_id is not None:
                    res_dict["msg_id"] = msg_id
                    
                results.append(res_dict)
            except Exception:
                pass
                
    return results
