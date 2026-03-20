"""Simple Postgres storage for user-entered stock prices."""
import os, logging, json
from datetime import datetime

logger = logging.getLogger(__name__)


def _get_conn():
    """Get a Postgres connection using Railway's DATABASE_URL."""
    import psycopg2
    url = os.getenv("DATABASE_URL")
    if not url:
        logger.warning("DATABASE_URL not set — prices won't persist")
        return None
    try:
        return psycopg2.connect(url)
    except Exception as e:
        logger.error(f"Postgres connection failed: {e}")
        return None


def init_db():
    """Create tables if they don't exist."""
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_prices (
                ticker VARCHAR(20) PRIMARY KEY,
                price DECIMAL(12,2) NOT NULL,
                entered_at TIMESTAMP DEFAULT NOW(),
                notes TEXT DEFAULT ''
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                price DECIMAL(12,2) NOT NULL,
                recorded_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database tables initialized")
    except Exception as e:
        logger.error(f"DB init error: {e}")
        conn.close()


def save_price(ticker: str, price: float, notes: str = "") -> bool:
    """Save or update a user-entered price."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        # Upsert current price
        cur.execute("""
            INSERT INTO user_prices (ticker, price, entered_at, notes)
            VALUES (%s, %s, NOW(), %s)
            ON CONFLICT (ticker) DO UPDATE SET
                price = EXCLUDED.price,
                entered_at = NOW(),
                notes = EXCLUDED.notes
        """, (ticker.upper(), price, notes))
        # Also record in history
        cur.execute("""
            INSERT INTO price_history (ticker, price)
            VALUES (%s, %s)
        """, (ticker.upper(), price))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Save price error: {e}")
        conn.close()
        return False


def save_prices_bulk(prices: dict) -> bool:
    """Save multiple prices at once. prices = {ticker: price}."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        for ticker, price in prices.items():
            if price and float(price) > 0:
                cur.execute("""
                    INSERT INTO user_prices (ticker, price, entered_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (ticker) DO UPDATE SET
                        price = EXCLUDED.price,
                        entered_at = NOW()
                """, (ticker.upper(), float(price)))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Bulk save error: {e}")
        conn.close()
        return False


def get_all_prices() -> dict:
    """Get all user-entered prices. Returns {ticker: {price, entered_at}}."""
    conn = _get_conn()
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT ticker, price, entered_at FROM user_prices ORDER BY ticker")
        result = {}
        for row in cur.fetchall():
            result[row[0]] = {
                "price": float(row[1]),
                "entered_at": row[2].isoformat() if row[2] else None,
            }
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Get prices error: {e}")
        conn.close()
        return {}


def delete_price(ticker: str) -> bool:
    """Delete a user-entered price."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_prices WHERE ticker = %s", (ticker.upper(),))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Delete price error: {e}")
        conn.close()
        return False
