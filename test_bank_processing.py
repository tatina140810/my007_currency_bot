import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime
import sys
import os

sys.path.append(os.getcwd())

from app.handlers.operations import handle_text
from app.core.config import REPORT_CHAT_ID

class TestBankProcessing(unittest.IsolatedAsyncioTestCase):
    async def test_bank_income_report_chat(self):
        # MESSAGE from REPORT_CHAT_ID
        # Should NOT require group tag
        # Should use message.date
        
        fixed_date = datetime(2025, 5, 20, 10, 0, 0)
        
        update = MagicMock()
        update.effective_chat.id = REPORT_CHAT_ID
        update.effective_chat.type = "channel" # or group
        update.effective_user.id = 999 
        update.effective_user.is_bot = False
        update.effective_message.text = "Поступление 5000 USD от Client A"
        update.effective_message.date = fixed_date
        # IMPORTANT: Set forward_date to None so "or" works correctly
        update.effective_message.forward_date = None 
        update.effective_message.reply_text = AsyncMock()

        with patch("app.handlers.operations.is_staff", return_value=True), \
             patch("app.handlers.operations.db") as mock_db, \
             patch("app.handlers.operations.looks_like_bank_income", return_value=True), \
             patch("app.handlers.operations.parse_income_notification", return_value={
                 "amount": 5000, "currency": "USD", "description": "Bank In"
             }), \
             patch("app.handlers.operations.queue_operation", new_callable=AsyncMock) as mock_queue:
            
            await handle_text(update, MagicMock())
            
            # Verify queue_operation called with correct ID and TIMESTAMP
            mock_queue.assert_called_once()
            args = mock_queue.call_args 
            # args[0] = (chat_id, type, currency, amount, desc), kwargs={'timestamp': ...}
            
            call_args = args[0]
            call_kwargs = args[1]
            
            self.assertEqual(call_args[0], REPORT_CHAT_ID)
            self.assertEqual(call_args[1], "Поступление")
            self.assertEqual(call_kwargs.get("timestamp"), fixed_date)

    async def test_forwarded_bank_message(self):
        # MESSAGE Forwarded to Group
        # Should use forward_date
        
        original_date = datetime(2025, 1, 1, 12, 0, 0)
        forward_date = datetime(2025, 1, 2, 12, 0, 0) # Later date
        
        update = MagicMock()
        update.effective_chat.id = 555 # Client Group
        update.effective_chat.type = "group"
        update.effective_user.id = 999 
        update.effective_user.is_bot = False
        update.effective_message.text = "Поступление 100 USD"
        update.effective_message.date = forward_date
        update.effective_message.forward_date = original_date # Key!
        update.effective_message.reply_text = AsyncMock()

        with patch("app.handlers.operations.is_staff", return_value=True), \
             patch("app.handlers.operations.db") as mock_db, \
             patch("app.handlers.operations.looks_like_bank_income", return_value=True), \
             patch("app.handlers.operations.parse_income_notification", return_value={
                 "amount": 100, "currency": "USD", "description": "Fwd In"
             }), \
             patch("app.handlers.operations.queue_operation", new_callable=AsyncMock) as mock_queue:
            
            await handle_text(update, MagicMock())
            
            # Verify queue_operation called with original_date
            args = mock_queue.call_args 
            call_kwargs = args[1]
            self.assertEqual(call_kwargs.get("timestamp"), original_date)

    async def test_bank_income_other_chat_private(self):
        # MESSAGE from Private Chat (NOT REPORT_CHAT_ID)
        # Should REQUIRE group tag
        
        update = MagicMock()
        update.effective_chat.id = 123
        update.effective_chat.type = "private"
        update.effective_user.id = 999
        update.effective_user.is_bot = False
        update.effective_message.text = "поступили 100 USD" # No group tag
        update.effective_message.date = datetime.now()
        update.effective_message.forward_date = None
        update.effective_message.reply_text = AsyncMock()

        with patch("app.handlers.operations.is_staff", return_value=True), \
             patch("app.handlers.operations.db") as mock_db, \
             patch("app.handlers.operations.looks_like_bank_income", return_value=True), \
             patch("app.handlers.operations.parse_income_notification", return_value={
                 "amount": 100, "currency": "USD", "description": "In"
             }), \
             patch("app.handlers.operations.queue_operation", new_callable=AsyncMock) as mock_queue:
            
            # mock extract_group_tag to return None
            with patch("app.handlers.operations.extract_group_tag", return_value=(None, "text")):
                await handle_text(update, MagicMock())
                
                # Should reply error "specify group"
                update.effective_message.reply_text.assert_called()
                self.assertIn("В личном чате укажи группу", update.effective_message.reply_text.call_args[0][0])
                mock_queue.assert_not_called()

if __name__ == "__main__":
    unittest.main()
