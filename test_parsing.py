import re

def normalize_currency(curr: str) -> str:
    """Mock normalize"""
    if not curr: return ""
    c = curr.strip().lower().replace(".", "").replace(",", "")
    if c in ("рубли", "руб"): return "RUB"
    if c in ("евро", "eur"): return "EUR"
    if c in ("usd", "usdt", "$"): return "USD"
    if c in ("cny", "юань"): return "CNY"
    if c in ("aed", "дирхам"): return "AED"
    return c.upper()

def parse_human_number(s: str) -> float:
    """Mock parse"""
    s = s.replace("\u00A0", " ").strip()
    s = re.sub(r"\s+", "", s)
    return float(s)

def test_parsing(text):
    print(f"Testing input: {text!r}")
    
    text = text.replace("\u00A0", " ")
    
    # Copy-paste logic from handler
    curr_pattern = r"(?:[a-zA-Zа-яА-Я]{2,}|[$€¥₽])"
    
    pattern_curr_first = re.compile(
        rf"(?P<c>{curr_pattern})\s+(?P<a>[\d\s.,]+?)(?=\s+{curr_pattern}|\s*$)"
    )
    
    pattern_amount_first = re.compile(
        rf"(?P<a>[\d\s.,]+?)\s+(?P<c>{curr_pattern})(?=\s+[\d\s.,]|\s*$)"
    )

    matches_cf = list(pattern_curr_first.finditer(text))
    matches_af = list(pattern_amount_first.finditer(text))
    
    final_matches = []
    if len(matches_cf) >= len(matches_af) and len(matches_cf) > 0:
        final_matches = matches_cf
        print("Strategy: Currency First")
    elif len(matches_af) > 0:
        final_matches = matches_af
        print("Strategy: Amount First")
    else:
        print("Strategy: Fallback")

    parsed = {}
    for m in final_matches:
        raw_curr = m.group("c")
        raw_amount = m.group("a")
        print(f"  Match: Curr='{raw_curr}' Amount='{raw_amount}'")
        try:
            val = parse_human_number(raw_amount)
            curr = normalize_currency(raw_curr)
            if curr: parsed[curr] = val
        except Exception as e:
            print(f"  Error parsing: {e}")
            
    print(f"Result: {parsed}\n")

if __name__ == "__main__":
    # Test case from user
    input_text = "Рубли 12027 694.000 USD 181 361.67 Евро 59 031.24 CNY 13 200.69 ДИРХАМ 280.00"
    test_parsing(input_text)
