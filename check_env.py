import os
import sys

print("Checking environment...")

# 1. Check openpyxl
try:
    import openpyxl
    print(f"openpyxl version: {openpyxl.__version__}")
except ImportError:
    print("ERROR: openpyxl not installed!")
    sys.exit(1)
except Exception as e:
    print(f"ERROR importing openpyxl: {e}")
    sys.exit(1)

# 2. Check outputs directory
try:
    os.makedirs("outputs", exist_ok=True)
    test_path = os.path.join("outputs", "test_write.txt")
    with open(test_path, "w") as f:
        f.write("test")
    os.remove(test_path)
    print("outputs directory is writable.")
except Exception as e:
    print(f"ERROR: outputs directory issue: {e}")
    sys.exit(1)

# 3. Check DB connection
try:
    import sqlite3
    conn = sqlite3.connect("currency_operations.db")
    print("DB connection OK")
    conn.close()
except Exception as e:
    print(f"ERROR connecting to DB: {e}")
    sys.exit(1)

print("Environment OK")
