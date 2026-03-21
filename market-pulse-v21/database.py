"""Postgres storage for user prices + paper portfolio tracking."""
import os, logging, json
from datetime import datetime, date

logger = logging.getLogger(__name__)


def _get_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL")
    if not url:
        logger.warning("DATABASE_URL not set")
        return None
    try:
        return psycopg2.connect(url)
    except Exception as e:
        logger.error(f"Postgres connection failed: {e}")
        return None


def init_db():
    conn = _get_conn()
    if not conn: return
    try:
        cur = conn.cursor()

        # User-entered prices (screener)
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

        # Paper portfolio snapshots
        cur.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id SERIAL PRIMARY KEY,
                name VARCHAR(20) NOT NULL UNIQUE,
                lock_date DATE NOT NULL,
                close_date DATE,
                iwm_entry_price DECIMAL(12,2),
                iwm_exit_price DECIMAL(12,2),
                status VARCHAR(10) DEFAULT 'active',
                total_return DECIMAL(8,2),
                benchmark_return DECIMAL(8,2),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Holdings in each portfolio snapshot
        cur.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_holdings (
                id SERIAL PRIMARY KEY,
                portfolio_name VARCHAR(20) NOT NULL,
                ticker VARCHAR(20) NOT NULL,
                entry_price DECIMAL(12,2) NOT NULL,
                ncav_per_share DECIMAL(12,2),
                p_ncav DECIMAL(8,4),
                net_cash_per_share DECIMAL(12,2),
                current_ratio DECIMAL(8,2),
                debt_to_equity DECIMAL(8,2),
                net_margin DECIMAL(8,2),
                burn_severity VARCHAR(10),
                exit_price DECIMAL(12,2),
                exit_reason VARCHAR(50),
                exit_date DATE,
                stock_return DECIMAL(8,2),
                UNIQUE(portfolio_name, ticker)
            )
        """)

        # Monthly price updates for active portfolios
        cur.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_updates (
                id SERIAL PRIMARY KEY,
                portfolio_name VARCHAR(20) NOT NULL,
                update_date DATE NOT NULL,
                ticker VARCHAR(20) NOT NULL,
                current_price DECIMAL(12,2) NOT NULL,
                iwm_price DECIMAL(12,2),
                UNIQUE(portfolio_name, update_date, ticker)
            )
        """)

        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database tables initialized (incl. portfolio)")
    except Exception as e:
        logger.error(f"DB init error: {e}")
        conn.close()


# ═══════════════════════════════════════════════════
# USER PRICES (screener)
# ═══════════════════════════════════════════════════

def save_price(ticker, price, notes=""):
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_prices (ticker, price, entered_at, notes) VALUES (%s, %s, NOW(), %s)
            ON CONFLICT (ticker) DO UPDATE SET price=EXCLUDED.price, entered_at=NOW(), notes=EXCLUDED.notes
        """, (ticker.upper(), price, notes))
        cur.execute("INSERT INTO price_history (ticker, price) VALUES (%s, %s)", (ticker.upper(), price))
        conn.commit(); cur.close(); conn.close()
        return True
    except Exception as e:
        logger.error(f"Save price: {e}"); conn.close(); return False

def save_prices_bulk(prices):
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        for t, p in prices.items():
            if p and float(p) > 0:
                cur.execute("""
                    INSERT INTO user_prices (ticker, price, entered_at) VALUES (%s, %s, NOW())
                    ON CONFLICT (ticker) DO UPDATE SET price=EXCLUDED.price, entered_at=NOW()
                """, (t.upper(), float(p)))
        conn.commit(); cur.close(); conn.close()
        return True
    except Exception as e:
        logger.error(f"Bulk save: {e}"); conn.close(); return False

def get_all_prices():
    conn = _get_conn()
    if not conn: return {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT ticker, price, entered_at FROM user_prices ORDER BY ticker")
        r = {row[0]: {"price": float(row[1]), "entered_at": row[2].isoformat() if row[2] else None} for row in cur.fetchall()}
        cur.close(); conn.close()
        return r
    except Exception as e:
        logger.error(f"Get prices: {e}"); conn.close(); return {}

def delete_price(ticker):
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_prices WHERE ticker = %s", (ticker.upper(),))
        conn.commit(); cur.close(); conn.close()
        return True
    except Exception as e:
        logger.error(f"Delete price: {e}"); conn.close(); return False


# ═══════════════════════════════════════════════════
# PAPER PORTFOLIO
# ═══════════════════════════════════════════════════

def lock_portfolio(name, holdings, iwm_price):
    """
    Lock in a new portfolio snapshot.
    name: e.g. "Q2-2026"
    holdings: list of dicts with ticker, entry_price, ncav_per_share, p_ncav, etc.
    iwm_price: Russell 2000 benchmark price at lock date
    """
    conn = _get_conn()
    if not conn: return {"error": "No DB connection"}
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO portfolio_snapshots (name, lock_date, iwm_entry_price, status)
            VALUES (%s, CURRENT_DATE, %s, 'active')
            ON CONFLICT (name) DO NOTHING
        """, (name, iwm_price))

        for h in holdings:
            cur.execute("""
                INSERT INTO portfolio_holdings
                    (portfolio_name, ticker, entry_price, ncav_per_share, p_ncav,
                     net_cash_per_share, current_ratio, debt_to_equity, net_margin, burn_severity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (portfolio_name, ticker) DO NOTHING
            """, (name, h["ticker"], h["entry_price"], h.get("ncav_per_share"),
                  h.get("p_ncav"), h.get("net_cash_per_share"), h.get("current_ratio"),
                  h.get("debt_to_equity"), h.get("net_margin"), h.get("burn_severity")))

        conn.commit(); cur.close(); conn.close()
        return {"ok": True, "portfolio": name, "holdings": len(holdings)}
    except Exception as e:
        logger.error(f"Lock portfolio: {e}"); conn.close()
        return {"error": str(e)}


def update_portfolio_prices(name, prices, iwm_price):
    """
    Monthly price update for an active portfolio.
    prices: {ticker: current_price}
    iwm_price: current IWM price
    """
    conn = _get_conn()
    if not conn: return {"error": "No DB connection"}
    try:
        cur = conn.cursor()
        today = date.today()
        for ticker, price in prices.items():
            cur.execute("""
                INSERT INTO portfolio_updates (portfolio_name, update_date, ticker, current_price, iwm_price)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (portfolio_name, update_date, ticker) DO UPDATE SET
                    current_price = EXCLUDED.current_price, iwm_price = EXCLUDED.iwm_price
            """, (name, today, ticker.upper(), float(price), float(iwm_price)))
        conn.commit(); cur.close(); conn.close()
        return {"ok": True, "updated": len(prices)}
    except Exception as e:
        logger.error(f"Update portfolio: {e}"); conn.close()
        return {"error": str(e)}


def exit_holding(portfolio_name, ticker, exit_price, reason="held to maturity"):
    """Mark a single holding as exited with final price and reason."""
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        # Calculate return
        cur.execute("""
            UPDATE portfolio_holdings SET
                exit_price = %s,
                exit_reason = %s,
                exit_date = CURRENT_DATE,
                stock_return = ROUND((%s - entry_price) / entry_price * 100, 2)
            WHERE portfolio_name = %s AND ticker = %s
        """, (exit_price, reason, exit_price, portfolio_name, ticker.upper()))
        conn.commit(); cur.close(); conn.close()
        return True
    except Exception as e:
        logger.error(f"Exit holding: {e}"); conn.close(); return False


def close_portfolio(name, iwm_exit_price):
    """
    Close a portfolio after 12 months.
    Calculates total return and benchmark return.
    """
    conn = _get_conn()
    if not conn: return {"error": "No DB connection"}
    try:
        cur = conn.cursor()

        # Get all holdings and their returns
        cur.execute("""
            SELECT ticker, entry_price, exit_price, stock_return
            FROM portfolio_holdings WHERE portfolio_name = %s
        """, (name,))
        holdings = cur.fetchall()

        # Calculate equal-weight portfolio return
        returns = [float(h[3]) for h in holdings if h[3] is not None]
        avg_return = round(sum(returns) / len(returns), 2) if returns else 0

        # Get entry IWM price
        cur.execute("SELECT iwm_entry_price FROM portfolio_snapshots WHERE name = %s", (name,))
        row = cur.fetchone()
        iwm_entry = float(row[0]) if row and row[0] else 0
        benchmark_return = round((float(iwm_exit_price) - iwm_entry) / iwm_entry * 100, 2) if iwm_entry > 0 else 0

        # Update snapshot
        cur.execute("""
            UPDATE portfolio_snapshots SET
                status = 'closed',
                close_date = CURRENT_DATE,
                iwm_exit_price = %s,
                total_return = %s,
                benchmark_return = %s
            WHERE name = %s
        """, (iwm_exit_price, avg_return, benchmark_return, name))

        conn.commit(); cur.close(); conn.close()
        return {"ok": True, "portfolio": name, "total_return": avg_return,
                "benchmark_return": benchmark_return, "excess": round(avg_return - benchmark_return, 2)}
    except Exception as e:
        logger.error(f"Close portfolio: {e}"); conn.close()
        return {"error": str(e)}


def get_all_portfolios():
    """Get all portfolio snapshots with their holdings and update history."""
    conn = _get_conn()
    if not conn: return []
    try:
        cur = conn.cursor()

        # Get all snapshots
        cur.execute("""
            SELECT name, lock_date, close_date, iwm_entry_price, iwm_exit_price,
                   status, total_return, benchmark_return
            FROM portfolio_snapshots ORDER BY lock_date DESC
        """)
        snapshots = []
        for row in cur.fetchall():
            name = row[0]
            snap = {
                "name": name,
                "lock_date": row[1].isoformat() if row[1] else None,
                "close_date": row[2].isoformat() if row[2] else None,
                "iwm_entry": float(row[3]) if row[3] else None,
                "iwm_exit": float(row[4]) if row[4] else None,
                "status": row[5],
                "total_return": float(row[6]) if row[6] else None,
                "benchmark_return": float(row[7]) if row[7] else None,
                "excess_return": round(float(row[6]) - float(row[7]), 2) if row[6] and row[7] else None,
                "holdings": [],
                "updates": [],
            }

            # Get holdings
            cur.execute("""
                SELECT ticker, entry_price, ncav_per_share, p_ncav,
                       net_cash_per_share, current_ratio, debt_to_equity,
                       net_margin, burn_severity, exit_price, exit_reason,
                       exit_date, stock_return
                FROM portfolio_holdings WHERE portfolio_name = %s ORDER BY ticker
            """, (name,))
            for h in cur.fetchall():
                snap["holdings"].append({
                    "ticker": h[0], "entry_price": float(h[1]),
                    "ncav_per_share": float(h[2]) if h[2] else None,
                    "p_ncav": float(h[3]) if h[3] else None,
                    "net_cash_per_share": float(h[4]) if h[4] else None,
                    "current_ratio": float(h[5]) if h[5] else None,
                    "debt_to_equity": float(h[6]) if h[6] else None,
                    "net_margin": float(h[7]) if h[7] else None,
                    "burn_severity": h[8],
                    "exit_price": float(h[9]) if h[9] else None,
                    "exit_reason": h[10],
                    "exit_date": h[11].isoformat() if h[11] else None,
                    "stock_return": float(h[12]) if h[12] else None,
                })

            # Get monthly updates
            cur.execute("""
                SELECT DISTINCT update_date, iwm_price FROM portfolio_updates
                WHERE portfolio_name = %s ORDER BY update_date
            """, (name,))
            update_dates = [(r[0], float(r[1]) if r[1] else None) for r in cur.fetchall()]

            for ud, iwm_p in update_dates:
                cur.execute("""
                    SELECT ticker, current_price FROM portfolio_updates
                    WHERE portfolio_name = %s AND update_date = %s
                """, (name, ud))
                prices = {r[0]: float(r[1]) for r in cur.fetchall()}

                # Calculate portfolio value on this date
                total_return = 0
                count = 0
                for holding in snap["holdings"]:
                    t = holding["ticker"]
                    if t in prices and holding["entry_price"] > 0:
                        ret = (prices[t] - holding["entry_price"]) / holding["entry_price"] * 100
                        total_return += ret
                        count += 1

                avg_ret = round(total_return / count, 2) if count > 0 else 0
                iwm_ret = round((iwm_p - snap["iwm_entry"]) / snap["iwm_entry"] * 100, 2) if iwm_p and snap["iwm_entry"] else 0

                snap["updates"].append({
                    "date": ud.isoformat(),
                    "portfolio_return": avg_ret,
                    "benchmark_return": iwm_ret,
                    "excess": round(avg_ret - iwm_ret, 2),
                    "iwm_price": iwm_p,
                    "prices": prices,
                })

            snapshots.append(snap)

        cur.close(); conn.close()
        return snapshots
    except Exception as e:
        logger.error(f"Get portfolios: {e}"); conn.close()
        return []
