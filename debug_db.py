import sqlite3
import os

db_path = "operations.db"
if not os.path.exists(db_path):
    print(f"Error: {db_path} not found")
    exit(1)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("--- TOP 10 RUB OPERATIONS BY AMOUNT ---")
cur.execute("SELECT timestamp, operation_type, amount, currency, description FROM operations WHERE currency='RUB' ORDER BY ABS(amount) DESC LIMIT 10")
rows = cur.fetchall()
for r in rows:
    print(r)

print("\n--- DISTINCT OPERATION TYPES ---")
cur.execute("SELECT DISTINCT operation_type FROM operations")
rows = cur.fetchall()
for r in rows:
    print(r[0])

conn.close()
