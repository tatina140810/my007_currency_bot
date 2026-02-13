import unittest
import os
import shutil
from datetime import datetime
# from database import Database # LEGACY
from app.db.database import Database # NEW

# Mock config
import sys
# We need to make sure we can import from app
sys.path.append(os.getcwd())

class TestCashReport(unittest.TestCase):
    def setUp(self):
        self.db_name = "test_cash.db"
        self.db = Database(self.db_name)
        self.db.clear_all()
        # Create tables again as clear_all might drop them or just delete data
        # Database.clear_all deletes data from operations and chats.
        # It doesn't touch new tables possibly. 
        # Let's ensure tables exist.
        self.db.create_tables()

    def tearDown(self):
        # self.db.close() # Database doesn't have close method
        if os.path.exists(self.db_name):
            os.remove(self.db_name)

    def test_debug_methods(self):
        print("DEBUG: Database methods:", [m for m in dir(self.db) if not m.startswith('_')])

    def test_opening_balances(self):
        today = datetime.now().strftime("%Y-%m-%d")
        self.db.set_cash_opening_balance(today, "USD", 1000.0)
        self.db.set_cash_opening_balance(today, "EUR", 500.0)
        
        balances = self.db.get_cash_opening_balances(today)
        self.assertEqual(balances["USD"], 1000.0)
        self.assertEqual(balances["EUR"], 500.0)
        
    def test_internal_rates(self):
        self.db.set_internal_rate("USD", "RUB", 90.0, group_id=1)
        rate = self.db.get_internal_rate("USD", "RUB", group_id=1)
        self.assertEqual(rate, 90.0)
        
        rate_none = self.db.get_internal_rate("USD", "KGS", group_id=1)
        self.assertIsNone(rate_none)

    def test_report_logic(self):
        today = datetime.now().strftime("%Y-%m-%d")
        group_id = 123
        
        chat_id = 100
        
        # 1. Opening Balance for THIS group
        self.db.set_cash_opening_balance(today, "USD", 1000.0, group_id=chat_id) 
        
        self.db.register_chat(chat_id, "TestChat", "private")
        
        # Cash Deposit
        self.db.add_operation(chat_id, "Взнос наличными", "USD", 500.0, "Deposit")
        
        # Cash Withdrawal
        self.db.add_operation(chat_id, "Выдача наличных", "USD", -200.0, "Withdrawal")
        
        # Bank Income (should comply with user request to include in report as withdrawal/subtraction)
        self.db.add_operation(chat_id, "Поступление", "USD", 300.0, "Bank Income")
        
        # Internal Exchange
        # USD -> RUB @ 90
        # -100 USD
        self.db.add_operation(chat_id, "Internal Exchange", "USD", -100.0, "Exchange Out")
        # +9000 RUB
        self.db.add_operation(chat_id, "Internal Exchange", "RUB", 9000.0, "Exchange In")
        
        # Verify Report Logic
        try:
             # Import first
             import app.services.cash
             from app.services.cash import get_report_data
             
             # Patch db in the module
             old_db_ref = app.services.cash.db
             app.services.cash.db = self.db
             
             # Call synchronously!
             # We pass group_id=0 to get ALL operations (or we could pass chat_id to test filtering)
             # Let's test filtering: Pass chat_id=100. It should return results.
             data = get_report_data(datetime.now(), group_id=chat_id)
             
             # Restore
             app.services.cash.db = old_db_ref

             if data is None:
                 self.fail("get_report_data returned None. Opening balances check failed?")
             
             # Logic:
             # Opening USD: 1000
             # Deposit USD: 500 (Cash "Взнос наличными")
             # Withdraw USD: 200 (Cash "Выдача") + 300 (Bank "Поступление") = 500
             # Exchange Out USD: 100
             # Closing USD = 1000 (Open) + 500 (Dep) - 500 (With) - 100 (Exch) + 0 (Exch In) = 900
             
             # RUB:
             # Opening: 0
             # Exchange In: 9000
             # Closing: 9000
             
             usd_data = data["summary"]["USD"]
             self.assertEqual(usd_data["opening"], 1000.0)
             self.assertEqual(usd_data["deposit"], 500.0)
             self.assertEqual(usd_data["withdraw"], 500.0)
             self.assertEqual(usd_data["exchange_out"], 100.0)
             self.assertEqual(usd_data["closing"], 900.0)
             
             rub_data = data["summary"]["RUB"]
             self.assertEqual(rub_data["exchange_in"], 9000.0)
             self.assertEqual(rub_data["closing"], 9000.0)
             
             # NEW: Verify Details
             self.assertIn("all_operations", data)
             all_ops = data["all_operations"]
             self.assertEqual(len(all_ops), 5) 
             
             # Test Filtering: Query for random group_id -> Should be empty/None
             app.services.cash.db = self.db
             data_empty = get_report_data(datetime.now(), group_id=999)
             app.services.cash.db = old_db_ref
             
             # Expect None because no opening balances for this group
             self.assertIsNone(data_empty)
             
        except ImportError:
            print("Could not import app.services.cash, skipping service test")
            raise
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.fail(f"Exception during test: {e}")

    def test_rep_global_income(self):
        """Test /rep command logic (get_report_income_by_date) with global search"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Chat 1: Private
        chat1 = 101
        self.db.register_chat(chat1, "PrivateUser", "private")
        self.db.add_operation(chat1, "Поступление", "USD", 100.0, "Payment from Client A")

        # Chat 2: Group
        chat2 = 102
        self.db.register_chat(chat2, "WorkingGroup", "group")
        # Manual operation without explicit client in text
        self.db.add_operation(chat2, "Поступление", "USD", 200.0, "Поступление 200 USD") 

        # Chat 3: Another Group
        chat3 = 103
        self.db.register_chat(chat3, "AnotherGroup", "group")
        self.db.add_operation(chat3, "Поступление", "EUR", 50.0, "Payment Client B")

        # Query with chat_id=None (Global)
        report = self.db.get_report_income_by_date(None, today)
        
        # report structure: [(client_name, currency, amount, full_text), ...]
        self.assertTrue(len(report) >= 3, "Should have at least 3 entries")

if __name__ == '__main__':
    unittest.main()
