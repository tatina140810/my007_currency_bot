import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os

# Ensure app imports work
sys.path.append(os.getcwd())

from app.handlers.operations import handle_text

class TestInternalReport(unittest.IsolatedAsyncioTestCase):
    async def test_manual_buy_fx(self):
        # [internal_report] 69000 EUR 91.8
        # Expected: +69000 EUR, -(69000*91.8) RUB
        
        update = MagicMock()
        update.effective_chat.id = 123
        update.effective_chat.type = "private"
        update.effective_user.id = 999 
        update.effective_user.is_bot = False
        update.effective_message.text = "[internal_report] 69000 EUR 91.8"
        update.effective_message.reply_text = AsyncMock()

        with patch("app.handlers.operations.is_staff", return_value=True), \
             patch("app.handlers.operations.db") as mock_db, \
             patch("app.handlers.operations.resolve_target_chat_id", side_effect=ValueError("Group needed")), \
             patch("app.handlers.operations.queue_operation", new_callable=AsyncMock) as mock_queue:
            
            await handle_text(update, MagicMock())
            
            # Expect 2 calls to queue_operation (Defaulted to chat 123)
            self.assertEqual(mock_queue.call_count, 2)
            
            # Call 1: +69000 EUR
            args1 = mock_queue.call_args_list[0][0]
            self.assertEqual(args1[0], 123)
            self.assertEqual(args1[1], "Internal Exchange")
            self.assertEqual(args1[2], "EUR")
            self.assertEqual(args1[3], 69000.0)
            
            # Call 2: -RUB
            args2 = mock_queue.call_args_list[1][0]
            self.assertEqual(args2[0], 123)
            self.assertEqual(args2[1], "Internal Exchange")
            self.assertEqual(args2[2], "RUB")
            self.assertEqual(args2[3], -(69000.0 * 91.8))
            
            # Verify reply
            update.effective_message.reply_text.assert_called_once()
            self.assertIn("[Internal Report] Buy FX", update.effective_message.reply_text.call_args[0][0])

    async def test_manual_cash_out(self):
        # [internal_report] наличные 5000 USD
        # Expected: -5000 USD (Выдача наличных)
        
        update = MagicMock()
        update.effective_chat.id = 123
        update.effective_chat.type = "private"
        update.effective_user.id = 999
        update.effective_user.is_bot = False
        update.effective_message.text = "[internal_report] наличные 5000 USD"
        update.effective_message.reply_text = AsyncMock()

        # Simulate resolve failing, but fallback logic working
        with patch("app.handlers.operations.is_staff", return_value=True), \
             patch("app.handlers.operations.db") as mock_db, \
             patch("app.handlers.operations.resolve_target_chat_id", side_effect=ValueError("Group needed")), \
             patch("app.handlers.operations.queue_operation", new_callable=AsyncMock) as mock_queue:
            
            await handle_text(update, MagicMock())
            
            # Expect 1 call
            self.assertEqual(mock_queue.call_count, 1)
            
            args = mock_queue.call_args[0]
            self.assertEqual(args[0], 123)
            self.assertEqual(args[1], "Выдача наличных")
            self.assertEqual(args[2], "USD")
            self.assertEqual(args[3], -5000.0)
            
            update.effective_message.reply_text.assert_called_once()
            self.assertIn("[Internal Report] Cash Out", update.effective_message.reply_text.call_args[0][0])

if __name__ == "__main__":
    unittest.main()
