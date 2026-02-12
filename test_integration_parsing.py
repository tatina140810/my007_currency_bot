import sys
import os
sys.path.append(os.getcwd())

from app.services.parser import normalize_currency, parse_human_number
import re

def test_real_parsing():
    input_text = "Рубли 12027 694.000 USD 181 361.67 Евро 59 031.24 CNY 13 200.69 ДИРХАМ 280.00"
    print(f"Testing input: {input_text}")
    
    # Simulate handler logic
    text = input_text.replace("\u00A0", " ")
    
    curr_pattern = r"(?:[a-zA-Zа-яА-Я]{2,}|[$€¥₽])"
    pattern_curr_first = re.compile(
        rf"(?P<c>{curr_pattern})\s+(?P<a>[\d\s.,]+?)(?=\s+{curr_pattern}|\s*$)"
    )
    
    matches = list(pattern_curr_first.finditer(text))
    parsed = {}
    
    for m in matches:
        raw_curr = m.group("c")
        raw_amount = m.group("a")
        print(f"  Match: {raw_curr} -> {raw_amount}")
        
        try:
            val = parse_human_number(raw_amount)
            curr = normalize_currency(raw_curr)
            print(f"    Parsed: {val} {curr}")
            if curr: parsed[curr] = val
        except Exception as e:
            print(f"    Error: {e}")

    print(f"Final: {parsed}")
    
    expected = {'RUB', 'USD', 'EUR', 'CNY', 'AED'}
    keys = set(parsed.keys())
    
    if keys != expected:
        print(f"FAIL: Expected keys {expected}, got {keys}")
        exit(1)
    
    if parsed["RUB"] != 12027694.0:
        print(f"FAIL: RUB amount wrong: {parsed['RUB']}")
        exit(1)

    print("SUCCESS")

if __name__ == "__main__":
    test_real_parsing()
