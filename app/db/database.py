"""
Модуль для работы с базой данных
"""

import sqlite3
import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple, Dict

from app.core.config import CURRENCIES, DB_PATH
from app.services.parser import extract_client_name

logger = logging.getLogger(__name__)

class Database:
    @staticmethod
    def _norm(s: str) -> str:
        s = (s or "").strip().lower()
        s = " ".join(s.split())
        return s

    def __init__(self, db_name: str = DB_PATH):
        """Инициализация базы данных"""
        self.db_name = db_name
        self.create_tables()

    def get_connection(self):
        """Создание стабильного подключения к SQLite (safe for asyncio)"""
        conn = sqlite3.connect(
            self.db_name,
            timeout=30,
            check_same_thread=False,
            isolation_level=None,   # автокоммит, снижает lock'и
        )

        conn.row_factory = sqlite3.Row

        # ⚠️ PRAGMA — строго в таком порядке
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=8000;")

        return conn

    def create_tables(self):
        """Создание таблиц в БД"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Таблица операций с chat_id
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                operation_type TEXT NOT NULL,
                currency TEXT NOT NULL,
                amount REAL NOT NULL,
                description TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Индекс для быстрого поиска по chat_id
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_operations_chat_id
            ON operations(chat_id)
        ''')

        # Таблица балансов с chat_id
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS balances (
                chat_id INTEGER NOT NULL,
                currency TEXT NOT NULL,
                balance REAL DEFAULT 0.0,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, currency)
            )
        ''')

        # Таблица с информацией о чатах
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                chat_name TEXT,
                chat_type TEXT,
                first_interaction DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_interaction DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # NEW: Таблица начальных остатков (Cash Evening Report)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cash_opening_balances (
                date TEXT NOT NULL,
                currency TEXT NOT NULL,
                amount REAL NOT NULL,
                group_id INTEGER DEFAULT 0,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, currency, group_id)
            )
        ''')

        # NEW: Таблица внутренних курсов (Cash Evening Report)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS internal_rates (
                group_id INTEGER DEFAULT 0,
                from_currency TEXT NOT NULL,
                to_currency TEXT NOT NULL,
                rate REAL NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_id, from_currency, to_currency)
            )
        ''')

        conn.commit()
        conn.close()

    def register_chat(self, chat_id: int, chat_name: str = None, chat_type: str = 'private'):
        """Регистрация чата/группы"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO chats (chat_id, chat_name, chat_type, last_interaction)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ''', (chat_id, chat_name, chat_type))

        # Инициализация балансов для всех валют для этого чата
        for currency in CURRENCIES:
            cursor.execute('''
                INSERT OR IGNORE INTO balances (chat_id, currency, balance)
                VALUES (?, ?, 0.0)
            ''', (chat_id, currency))

        conn.commit()
        conn.close()

    def add_operation(
        self,
        chat_id: int,
        operation_type: str,
        currency: str,
        amount: float,
        description: str = ""
    ) -> int:
        """
        Добавить операцию для конкретного чата
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        # Убедимся что чат зарегистрирован
        cursor.execute('SELECT chat_id FROM chats WHERE chat_id = ?', (chat_id,))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO chats (chat_id) VALUES (?)
            ''', (chat_id,))
            # Инициализация балансов
            for curr in CURRENCIES:
                cursor.execute('''
                    INSERT OR IGNORE INTO balances (chat_id, currency, balance)
                    VALUES (?, ?, 0.0)
                ''', (chat_id, curr))

        # Добавляем операцию
        cursor.execute('''
            INSERT INTO operations (chat_id, operation_type, currency, amount, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (chat_id, operation_type, currency, amount, description))

        operation_id = cursor.lastrowid

        # Обновляем баланс для этого чата
        cursor.execute('''
            INSERT INTO balances (chat_id, currency, balance, last_updated)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(chat_id, currency) DO UPDATE SET
                balance = balance + ?,
                last_updated = CURRENT_TIMESTAMP
        ''', (chat_id, currency, amount, amount))

        # Обновляем время последнего взаимодействия
        cursor.execute('''
            UPDATE chats SET last_interaction = CURRENT_TIMESTAMP WHERE chat_id = ?
        ''', (chat_id,))

        conn.commit()
        conn.close()

        return operation_id

    def get_balances(self, chat_id: int) -> Dict[str, float]:
        """Получить балансы для конкретного чата"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Убедимся что чат зарегистрирован в balances
        cursor.execute('SELECT chat_id FROM balances WHERE chat_id = ? LIMIT 1', (chat_id,))
        if not cursor.fetchone():
            # Инициализируем балансы
            for currency in CURRENCIES:
                cursor.execute('''
                    INSERT OR IGNORE INTO balances (chat_id, currency, balance)
                    VALUES (?, ?, 0.0)
                ''', (chat_id, currency))
            conn.commit()

        cursor.execute('''
            SELECT currency, balance
            FROM balances
            WHERE chat_id = ?
            ORDER BY currency
        ''', (chat_id,))
        rows = cursor.fetchall()

        conn.close()

        result = {row["currency"]: row["balance"] for row in rows}

        # Добавляем недостающие валюты из CURRENCIES
        for currency in CURRENCIES:
            if currency not in result:
                result[currency] = 0.0

        return result

    def get_balance(self, chat_id: int, currency: str) -> float:
        """Получить баланс по конкретной валюте для чата"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT balance FROM balances
            WHERE chat_id = ? AND currency = ?
        ''', (chat_id, currency))
        row = cursor.fetchone()

        conn.close()

        return row["balance"] if row else 0.0

    def get_group_balances_table(self) -> Dict[str, Dict[str, float]]:
        """
        Таблица остатков:
        {
          "Название группы": {"USD": 10, "RUB": 500, ...},
          ...
        }
        """
        conn = self.get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COALESCE(c.chat_name, CAST(b.chat_id AS TEXT)) AS group_name,
                b.currency,
                b.balance
            FROM balances b
            LEFT JOIN chats c ON c.chat_id = b.chat_id
            ORDER BY group_name, b.currency
        """)
        rows = cur.fetchall()
        conn.close()

        table = defaultdict(dict)
        for r in rows:
            table[r["group_name"]][r["currency"]] = float(r["balance"] or 0.0)

        # гарантируем все валюты из CURRENCIES
        for group_name in table:
            for currency in CURRENCIES:
                table[group_name].setdefault(currency, 0.0)
        
        # Фильтруем (у кого есть не ноль)
        filtered_table = {g: curmap for g, curmap in table.items()
                if any(abs(v) > 1e-9 for v in curmap.values())}

        return dict(filtered_table)
    
    def get_total_balances_all_groups(self) -> Dict[str, float]:
        """Итого по валютам по всем чатам/группам"""
        conn = self.get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT currency, COALESCE(SUM(balance), 0) AS total
            FROM balances
            GROUP BY currency
            ORDER BY currency
        """)
        rows = cur.fetchall()
        conn.close()

        result = {r["currency"]: float(r["total"] or 0.0) for r in rows}
        for cur_ in CURRENCIES:
            result.setdefault(cur_, 0.0)
        return result

    def get_operations(
        self,
        chat_id: int,
        limit: int = 20,
        currency: str | None = None
    ) -> List[Tuple]:
        """Получить список операций для конкретного чата"""
        conn = self.get_connection()
        cursor = conn.cursor()

        if currency:
            cursor.execute(
                '''
                SELECT id, operation_type, currency, amount, description,
                       strftime('%d.%m.%Y %H:%M', timestamp) as timestamp
                FROM operations
                WHERE chat_id = ? AND currency = ?
                ORDER BY id DESC
                LIMIT ?
                ''',
                (chat_id, currency, limit),
            )
        else:
            cursor.execute(
                '''
                SELECT id, operation_type, currency, amount, description,
                       strftime('%d.%m.%Y %H:%M', timestamp) as timestamp
                FROM operations
                WHERE chat_id = ?
                ORDER BY id DESC
                LIMIT ?
                ''',
                (chat_id, limit),
            )

        rows = cursor.fetchall()
        conn.close()

        return [
            (
                row["id"],
                row["operation_type"],
                row["currency"],
                row["amount"],
                row["description"],
                row["timestamp"],
            )
            for row in rows
        ]

    def get_operations_by_date(self, chat_id: int, date_from=None, date_to=None):
        conn = self.get_connection()
        cur = conn.cursor()

        logger.info(f"get_operations_by_date: chat_id={chat_id}, from={date_from}, to={date_to}")

        if date_from and date_to:
            date_from_str = date_from.strftime("%Y-%m-%d") if hasattr(date_from, 'strftime') else str(date_from)
            date_to_str = date_to.strftime("%Y-%m-%d") if hasattr(date_to, 'strftime') else str(date_to)

            cur.execute("""
                SELECT id, operation_type, currency, amount, description,
                    strftime('%d.%m.%Y %H:%M', timestamp) as timestamp
                FROM operations
                WHERE chat_id = ?
                AND date(timestamp) BETWEEN date(?) AND date(?)
                ORDER BY timestamp
            """, (chat_id, date_from_str, date_to_str))

        elif date_from:
            date_from_str = date_from.strftime("%Y-%m-%d") if hasattr(date_from, 'strftime') else str(date_from)
            cur.execute("""
                SELECT id, operation_type, currency, amount, description,
                    strftime('%d.%m.%Y %H:%M', timestamp) as timestamp
                FROM operations
                WHERE chat_id = ?
                AND date(timestamp) = date(?)
                ORDER BY timestamp
            """, (chat_id, date_from_str))

        else:
            cur.execute("""
                SELECT id, operation_type, currency, amount, description,
                    strftime('%d.%m.%Y %H:%M', timestamp) as timestamp
                FROM operations
                WHERE chat_id = ?
                ORDER BY timestamp
            """, (chat_id,))

        rows = cur.fetchall()
        conn.close()

        return [
            (
                row["id"],
                row["operation_type"],
                row["currency"],
                row["amount"],
                row["description"],
                row["timestamp"],
            )
            for row in rows
        ]

    def get_statistics(self, chat_id: int) -> Dict[str, Dict[str, float]]:
        """Получить статистику для конкретного чата"""
        conn = self.get_connection()
        cursor = conn.cursor()

        stats: Dict[str, Dict[str, float]] = {}

        for currency in CURRENCIES:
            cursor.execute(
                '''
                SELECT COALESCE(SUM(amount), 0) as income
                FROM operations
                WHERE chat_id = ? AND currency = ? AND amount > 0
                ''',
                (chat_id, currency),
            )
            income = cursor.fetchone()["income"]

            cursor.execute(
                '''
                SELECT COALESCE(SUM(amount), 0) as expense
                FROM operations
                WHERE chat_id = ? AND currency = ? AND amount < 0
                ''',
                (chat_id, currency),
            )
            expense = cursor.fetchone()["expense"]

            balance = self.get_balance(chat_id, currency)

            if income != 0 or expense != 0 or balance != 0:
                stats[currency] = {
                    "income": income,
                    "expense": expense,
                    "balance": balance,
                }

        conn.close()
        return stats

    def get_total_operations_count(self, chat_id: int) -> int:
        """Получить общее количество операций для чата"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            '''
            SELECT COUNT(*) as count
            FROM operations
            WHERE chat_id = ?
            ''',
            (chat_id,),
        )
        count = cursor.fetchone()["count"]

        conn.close()
        return count

    def get_all_chats(self) -> List[Tuple]:
        """Получить список всех чатов"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            '''
            SELECT chat_id, chat_name, chat_type,
                   strftime('%d.%m.%Y %H:%M', first_interaction) as first_interaction,
                   strftime('%d.%m.%Y %H:%M', last_interaction) as last_interaction
            FROM chats
            ORDER BY last_interaction DESC
            '''
        )
        rows = cursor.fetchall()
        conn.close()
        return [
            (
                row["chat_id"],
                row["chat_name"],
                row["chat_type"],
                row["first_interaction"],
                row["last_interaction"],
            )
            for row in rows
        ]

    def get_chat(self, chat_id: int):
        """Получить один чат по chat_id"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT chat_id, chat_name, chat_type,
                strftime('%d.%m.%Y %H:%M', first_interaction) as first_interaction,
                strftime('%d.%m.%Y %H:%M', last_interaction) as last_interaction
            FROM chats
            WHERE chat_id = ?
        """, (chat_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        return (
            row["chat_id"],
            row["chat_name"],
            row["chat_type"],
            row["first_interaction"],
            row["last_interaction"],
        )

    def delete_operation(self, chat_id: int, operation_id: int) -> bool:
        """Удалить операцию (с откатом баланса)"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            '''
            SELECT currency, amount FROM operations
            WHERE id = ? AND chat_id = ?
            ''',
            (operation_id, chat_id),
        )
        row = cursor.fetchone()

        if not row:
            conn.close()
            return False

        currency = row["currency"]
        amount = row["amount"]

        cursor.execute(
            "DELETE FROM operations WHERE id = ? AND chat_id = ?",
            (operation_id, chat_id),
        )

        cursor.execute(
            '''
            UPDATE balances
            SET balance = balance - ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE chat_id = ? AND currency = ?
            ''',
            (amount, chat_id, currency),
        )

        conn.commit()
        conn.close()

        return True

    def get_chat_id_by_name(self, name: str) -> int | None:
        """Ищем chat_id по имени группы максимально устойчиво."""
        wanted = self._norm(name)
        if not wanted:
            return None

        chats = self.get_all_chats()
        best_id = None
        best_score = 0

        for (cid, chat_name, _chat_type, _fi, _li) in chats:
            n = self._norm(chat_name)

            if n == wanted:
                return cid

            score = 0
            if wanted in n:
                score = 100 + len(wanted)
            else:
                w_words = set(wanted.split())
                n_words = set(n.split())
                common = len(w_words & n_words)
                if common:
                    score = 10 * common

            if score > best_score:
                best_score = score
                best_id = cid

        return best_id if best_score >= 20 else None

    def clear_all(self):
        conn = self.get_connection()
        cur = conn.cursor()

        cur.execute("DELETE FROM operations;")
        cur.execute("DELETE FROM chats;")

        conn.commit()
        conn.close()

    def recalculate_balances(self, chat_id: int | None = None):
        """Пересчитать балансы"""
        conn = self.get_connection()
        cursor = conn.cursor()

        if chat_id is not None:
            chats = [(chat_id,)]
        else:
            cursor.execute("SELECT DISTINCT chat_id FROM operations")
            chats = cursor.fetchall()

        for (cid,) in chats:
            cursor.execute(
                '''
                UPDATE balances SET balance = 0.0
                WHERE chat_id = ?
                ''',
                (cid,),
            )

            for currency in CURRENCIES:
                cursor.execute(
                    '''
                    SELECT COALESCE(SUM(amount), 0) as total
                    FROM operations
                    WHERE chat_id = ? AND currency = ?
                    ''',
                    (cid, currency),
                )
                total = cursor.fetchone()["total"]

                cursor.execute(
                    '''
                    INSERT INTO balances (chat_id, currency, balance, last_updated)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(chat_id, currency) DO UPDATE SET
                        balance = ?,
                        last_updated = CURRENT_TIMESTAMP
                    ''',
                    (cid, currency, total, total),
                )

        conn.commit()
        conn.close()
    
    def get_report_income_by_date(self, chat_id: int | None, report_date: str):
        """
        Возвращает список строк для отчёта:
        [(client_name, currency, amount, full_message), ...]
        If chat_id is None, searches all chats.
        """
        conn = self.get_connection()
        cur = conn.cursor()

        # Modified query to JOIN with chats and handle optional chat_id
        sql_base = """
            SELECT
                COALESCE(NULLIF(TRIM(o.description), ''), 'Без клиента') AS full_message,
                o.currency,
                o.amount,
                c.chat_name
            FROM operations o
            LEFT JOIN chats c ON o.chat_id = c.chat_id
            WHERE o.amount > 0
            AND date(o.timestamp) = date(?)
        """
        
        params = [report_date]
        
        if chat_id is not None:
            sql_base += " AND o.chat_id = ?"
            params.append(chat_id)
            
        sql_base += " ORDER BY o.timestamp ASC"
        
        cur.execute(sql_base, tuple(params))

        rows = cur.fetchall()
        conn.close()

        agg = defaultdict(float)                 # (client_name, currency) -> sum
        msgs = defaultdict(list)                 # client_name -> list of full messages

        for r in rows:
            full_message = r["full_message"]
            cur_ = r["currency"]
            amt = float(r["amount"] or 0.0)
            chat_name = r["chat_name"] or ""

            client_name = extract_client_name(full_message)
            
            # Fallback to chat_name if "Без клиента"
            if client_name == "Без клиента" and chat_name:
                # Clean up chat name if needed
                client_name = chat_name

            key = (client_name, cur_)
            agg[key] += amt

            if full_message and full_message != "Без клиента":
                msgs[client_name].append(str(full_message))
            elif chat_name:
                 msgs[client_name].append(f"Чат: {chat_name}")

        out = []
        for (client_name, cur_), total_amt in sorted(agg.items(), key=lambda x: (x[0][0], x[0][1])):
            full_text = "\n\n---\n\n".join(msgs.get(client_name, []))
            out.append((client_name, cur_, float(total_amt), full_text))

        return out

    def migrate_legacy_data(self):
        """Миграция старых валют"""
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("""
                UPDATE operations
                SET currency = 'CNY'
                WHERE currency IN ('ЮАНЬ', 'ЮАНЕЙ', 'ЮАНЯ', 'ЮАН');
            """)
            conn.commit()
            conn.close()
            logger.info("Миграция валют выполнена (via DB class)")
        except Exception as e:
            logger.error(f"Ошибка миграции валют: {e}")

    def set_cash_opening_balance(self, date_str: str, currency: str, amount: float, group_id: int = 0):
        """
        Установить начальный остаток для кассы
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO cash_opening_balances (date, currency, amount, group_id, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (date_str, currency, amount, group_id))
        
        conn.commit()
        conn.close()

    def get_cash_opening_balances(self, date_str: str, group_id: int = 0) -> Dict[str, float]:
        """
        Получить все начальные остатки на дату
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT currency, amount FROM cash_opening_balances
            WHERE date = ? AND group_id = ?
        ''', (date_str, group_id))
        
        rows = cursor.fetchall()
        conn.close()
        
        return {row["currency"]: row["amount"] for row in rows}

    def set_internal_rate(self, from_curr: str, to_curr: str, rate: float, group_id: int = 0):
        """
        Установить внутренний курс
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO internal_rates (group_id, from_currency, to_currency, rate, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (group_id, from_curr, to_curr, rate))
        
        conn.commit()
        conn.close()

    def get_internal_rate(self, from_curr: str, to_curr: str, group_id: int = 0) -> float | None:
        """
        Получить внутренний курс. 
        Если прямого курса нет, можно попробовать обратный (1/rate), 
        но пока реализуем только прямой поиск.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT rate FROM internal_rates
            WHERE group_id = ? AND from_currency = ? AND to_currency = ?
        ''', (group_id, from_curr, to_curr))
        
        row = cursor.fetchone()
        conn.close()
        
        return row["rate"] if row else None

