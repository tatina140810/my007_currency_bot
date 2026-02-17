import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os

# Ensure app imports work
sys.path.append(os.getcwd())

from app.handlers.reports import cmd_rep

class TestRepConversion(unittest.IsolatedAsyncioTestCase):
    async def test_conversion_success(self):
        # Mock Update and Context
        update = MagicMock()
        update.effective_chat.id = 123
        update.effective_chat.type = "private"
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = ["100", "USD", "90"] # 100 USD at 90 RUB

        # Mock DB
        with patch("app.handlers.reports.db") as mock_db:
            # Setup initial balance
            # We need RUB balance since we are spending RUB to buy USD
            # 100 USD * 90 = 9000 RUB required
            def get_balance_side_effect(chat_id, currency):
                if currency == "RUB":
                    return 10000.0 # Enough RUB
                return 0.0

            mock_db.get_balance.side_effect = get_balance_side_effect
            
            # Run command
            await cmd_rep(update, context)
            
            # Verify DB calls
            # 1. Check balance for RUB
            mock_db.get_balance.assert_called_with(123, "RUB")
            
            # 2. Add operations
            # We expect 2 calls to add_operation
            self.assertEqual(mock_db.add_operation.call_count, 2)
            
            # Call 1: Add USD (+)
            args1, _ = mock_db.add_operation.call_args_list[0]
            # chat_id, type, currency, amount, desc
            self.assertEqual(args1[0], 123)
            self.assertEqual(args1[1], "Internal Exchange")
            self.assertEqual(args1[2], "USD")
            self.assertEqual(args1[3], 100.0) # POSITIVE 100 USD
            
            # Call 2: Deduct RUB (-)
            args2, _ = mock_db.add_operation.call_args_list[1]
            self.assertEqual(args2[0], 123)
            self.assertEqual(args2[1], "Internal Exchange")
            self.assertEqual(args2[2], "RUB")
            self.assertEqual(args2[3], -9000.0) # NEGATIVE 9000 RUB
            
            # Verify Success Message
            update.message.reply_text.assert_called_once()
            call_args = update.message.reply_text.call_args[0][0]
            self.assertIn("Конвертация выполнена", call_args)
            self.assertIn("USD: +100.00", call_args)
            self.assertIn("RUB: -9,000.00", call_args)

    async def test_conversion_insufficient_funds(self):
        update = MagicMock()
        update.effective_chat.id = 123
        update.effective_chat.type = "private"
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = ["1000", "EUR", "95"] # 1000 * 95 = 95000 RUB needed

        with patch("app.handlers.reports.db") as mock_db:
            # Not enough RUB
            mock_db.get_balance.return_value = 50.0 
            
            await cmd_rep(update, context)
            
            # Verify NO operations added
            mock_db.add_operation.assert_not_called()
            
            # Verify Error Message
            update.message.reply_text.assert_called_once()
            self.assertIn("Недостаточно RUB", update.message.reply_text.call_args[0][0])

    async def test_conversion_invalid_input(self):
        update = MagicMock()
        update.effective_chat.id = 123
        update.effective_chat.type = "private"
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = ["-100", "USD", "90"] # Negative amount

        with patch("app.handlers.reports.db") as mock_db:
             await cmd_rep(update, context)
             update.message.reply_text.assert_called_with("❌ Сумма и курс должны быть больше нуля.")

if __name__ == "__main__":
    unittest.main()
