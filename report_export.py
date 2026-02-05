from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter

def _autosize(ws):
    for col in ws.columns:
        mx = 0
        letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value is not None:
                mx = max(mx, len(str(cell.value)))
        ws.column_dimensions[letter].width = min(mx + 2, 45)

def export_report_income_matrix(rows, output_path: str, report_date: str):
    # rows: [(client, currency, amount), ...]
    agg = defaultdict(lambda: defaultdict(float))
    totals = defaultdict(float)

    for client, cur, amt in rows:
        agg[client][cur] += float(amt)
        totals[cur] += float(amt)

    currencies = sorted(totals.keys())
    clients = sorted(agg.keys())

    wb = Workbook()
    ws = wb.active
    ws.title = f"Report_{report_date}"

    ws["A1"] = "Клиент"
    ws["A1"].font = Font(bold=True)
    ws["A1"].alignment = Alignment(horizontal="center")

    for j, cur in enumerate(currencies, start=2):
        c = ws.cell(row=1, column=j, value=cur)
        c.font = Font(bold=True)
        c.alignment = Alignment(horizontal="center")

    ws.freeze_panes = "A2"

    r = 2
    for client in clients:
        ws.cell(row=r, column=1, value=client)
        for j, cur in enumerate(currencies, start=2):
            v = agg[client].get(cur, 0.0)
            cell = ws.cell(row=r, column=j, value=v if v != 0 else "")
            cell.number_format = "#,##0.00"
        r += 1

    ws.cell(row=r, column=1, value="ИТОГО").font = Font(bold=True)
    for j, cur in enumerate(currencies, start=2):
        cell = ws.cell(row=r, column=j, value=totals.get(cur, 0.0))
        cell.font = Font(bold=True)
        cell.number_format = "#,##0.00"

    _autosize(ws)
    wb.save(output_path)