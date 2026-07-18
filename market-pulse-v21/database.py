"""Postgres storage for user prices + paper portfolio tracking."""
import os, logging, json, time
from datetime import datetime, date

logger = logging.getLogger(__name__)

# Connection retry tuning. Railway's internal DNS / Postgres readiness
# occasionally lags during a deploy swap — we've seen the app start
# before postgres.railway.internal accepts TCP. Without retries the
# very first init_db() failed silently and every DB call for the
# container's lifetime returned {"error": "No DB connection"} (the
# code path callers swallow individually). A short retry loop with
# linear backoff converts that into a transient warning instead of
# a permanently-degraded container.
_CONN_RETRIES = 4
_CONN_BACKOFFS = (2, 4, 6, 10)   # seconds between attempts
_CONN_TIMEOUT = 8                 # per-attempt socket timeout


def _get_conn():
    """Open a fresh psycopg2 connection. Retries transient network /
    server-not-ready failures with linear backoff; returns None if
    DATABASE_URL isn't set or every retry exhausts.

    Per-call connection (not pooled) so this is also the same path
    individual request handlers take — they benefit from retries too,
    not just the boot-time init_db().
    """
    import psycopg2
    url = os.getenv("DATABASE_URL")
    if not url:
        logger.warning("DATABASE_URL not set")
        return None
    last_err: Exception | None = None
    for attempt in range(_CONN_RETRIES):
        try:
            return psycopg2.connect(url, connect_timeout=_CONN_TIMEOUT)
        except Exception as e:
            last_err = e
            if attempt < _CONN_RETRIES - 1:
                wait = _CONN_BACKOFFS[attempt]
                logger.warning(
                    "Postgres connect failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1, _CONN_RETRIES, e, wait,
                )
                time.sleep(wait)
    logger.error("Postgres connection failed after %d attempts: %s",
                 _CONN_RETRIES, last_err)
    return None


def _ensure_landscaper_tables():
    """Create the landscaper shared-book tables in their OWN connection
    and transaction.

    Deliberately separate from init_db()'s big single-transaction block:
    that block accretes post-launch ALTERs whose ordering only holds on
    an already-migrated DB, so on a fresh DB one bad statement aborts the
    whole transaction and rolls back everything after it. Keeping these
    tables self-contained means the landscaper book always initializes,
    regardless of the CRM migrations' state. All statements are
    IF NOT EXISTS, so this is idempotent.

    The unguessable book code IS the auth (same pattern as prototype
    feedback tokens): creation is admin-gated, all reads/writes require
    the code.
    """
    conn = _get_conn()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS landscaper_books (
                id         SERIAL PRIMARY KEY,
                code       VARCHAR(24) NOT NULL UNIQUE,
                name       VARCHAR(80),
                costs      TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS landscaper_clients (
                id         SERIAL PRIMARY KEY,
                book_code  VARCHAR(24) NOT NULL,
                name       VARCHAR(80) NOT NULL,
                zip        VARCHAR(5) NOT NULL,
                price      REAL NOT NULL,
                freq       VARCHAR(2) NOT NULL DEFAULT 'w',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS landscaper_clients_book_idx
            ON landscaper_clients(book_code)
        """)
        conn.commit(); cur.close()
    except Exception as e:
        logger.error(f"landscaper table init error: {e}")
    finally:
        conn.close()


def init_db():
    # Self-contained migration first, in its own transaction, so it can't
    # be rolled back by an unrelated failure in the block below.
    _ensure_landscaper_tables()
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

        # Sign-up list — Phase 1 of going-paid. Free for now, just
        # capturing emails so we can email people when paid features
        # launch. source tracks which page/feature drove the signup.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(120),
                source VARCHAR(60),
                user_agent VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── CRM (private sales pipeline) ──
        # Two-person internal use, admin-gated. Mirrors the data model
        # in BUILD_SPEC.md for Pipeline CRM. Money fields are integers
        # in whole dollars. Stage history goes in crm_stage_events so
        # analytics can compute conversion over any date range.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crm_contacts (
                id              SERIAL PRIMARY KEY,
                name            VARCHAR(160) NOT NULL,
                title           VARCHAR(120),
                agency          VARCHAR(160),
                email           VARCHAR(255),
                stage           VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
                pilot_value     INTEGER NOT NULL DEFAULT 0,
                recurring_value INTEGER NOT NULL DEFAULT 0,
                date_emailed    DATE,
                next_date       DATE,
                subject         VARCHAR(255),
                notes           TEXT,
                created_at      TIMESTAMP DEFAULT NOW(),
                updated_at      TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crm_stage_events (
                id          SERIAL PRIMARY KEY,
                contact_id  INTEGER NOT NULL REFERENCES crm_contacts(id) ON DELETE CASCADE,
                from_stage  VARCHAR(32),
                to_stage    VARCHAR(32) NOT NULL,
                occurred_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS crm_stage_events_occurred_idx
            ON crm_stage_events(occurred_at)
        """)
        # Funnel aggregates filter/group by to_stage on every /pipeline
        # render (WHERE to_stage = ANY(...) GROUP BY to_stage); index it
        # so those don't seq-scan the whole event log.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS crm_stage_events_to_stage_idx
            ON crm_stage_events(to_stage)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crm_weekly_goals (
                id         SERIAL PRIMARY KEY,
                week_start DATE NOT NULL,
                metric     VARCHAR(32) NOT NULL,
                target     INTEGER NOT NULL,
                UNIQUE(week_start, metric)
            )
        """)
        # Email templates per (industry, trigger).
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crm_email_templates (
                id          SERIAL PRIMARY KEY,
                industry    VARCHAR(80) NOT NULL,
                trigger     VARCHAR(40) NOT NULL,
                subject     VARCHAR(255) NOT NULL,
                body        TEXT NOT NULL,
                created_at  TIMESTAMP DEFAULT NOW(),
                updated_at  TIMESTAMP DEFAULT NOW(),
                UNIQUE(industry, trigger)
            )
        """)
        # Industry tag on existing contacts (added after launch — guard
        # with ADD COLUMN IF NOT EXISTS so re-init is idempotent).
        cur.execute("""
            ALTER TABLE crm_contacts
            ADD COLUMN IF NOT EXISTS industry VARCHAR(80)
        """)
        # Email-thread / async-correspondence bank per contact. Free
        # text the user pastes from email threads between calls so the
        # AI prototype-brief step has the full picture.
        cur.execute("""
            ALTER TABLE crm_contacts
            ADD COLUMN IF NOT EXISTS email_thread TEXT
        """)
        # Prototype brief artifact — Step 5 of the working-session chain.
        cur.execute("""
            ALTER TABLE crm_working_sessions
            ADD COLUMN IF NOT EXISTS prototype_brief TEXT
        """)
        # Step 6 — post-review iteration planning. Free-form feedback
        # the user pastes from a client review call, plus the AI's two
        # output prompts (Claude Code + claude.ai design).
        cur.execute("""
            ALTER TABLE crm_working_sessions
            ADD COLUMN IF NOT EXISTS iteration_feedback TEXT
        """)
        cur.execute("""
            ALTER TABLE crm_working_sessions
            ADD COLUMN IF NOT EXISTS iteration_code_prompt TEXT
        """)
        cur.execute("""
            ALTER TABLE crm_working_sessions
            ADD COLUMN IF NOT EXISTS iteration_design_prompt TEXT
        """)
        # Gap-analysis artifact — Step 2 of the discovery-call chain.
        cur.execute("""
            ALTER TABLE crm_discovery_calls
            ADD COLUMN IF NOT EXISTS gap_analysis_md TEXT
        """)
        # NURTURE play — after a prospect replies but says they've signed
        # with a competitor. We stash a follow_up_date (default +4 mo)
        # so we can reach back out with a "gaps in their new tool" pitch.
        cur.execute("""
            ALTER TABLE crm_contacts
            ADD COLUMN IF NOT EXISTS follow_up_date DATE
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS crm_contacts_follow_up_idx
            ON crm_contacts(follow_up_date)
            WHERE follow_up_date IS NOT NULL
        """)
        # A/B testing — multiple subject/body variants per (industry,
        # role, trigger). The existing UNIQUE(industry, role, trigger)
        # constraint gets dropped and replaced with (industry, role,
        # trigger, variant_label) so we can store A, B, C, ... per
        # template. Existing rows backfill to variant_label='A'.
        cur.execute("""
            ALTER TABLE crm_email_templates
            ADD COLUMN IF NOT EXISTS variant_label VARCHAR(8) NOT NULL DEFAULT 'A'
        """)
        cur.execute("""
            ALTER TABLE crm_email_templates
            ADD COLUMN IF NOT EXISTS variant_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE'
        """)
        cur.execute("""
            ALTER TABLE crm_email_templates
            ADD COLUMN IF NOT EXISTS sends_count INTEGER NOT NULL DEFAULT 0
        """)
        cur.execute("""
            ALTER TABLE crm_email_templates
            ADD COLUMN IF NOT EXISTS replies_count INTEGER NOT NULL DEFAULT 0
        """)
        cur.execute("""
            ALTER TABLE crm_email_templates
            DROP CONSTRAINT IF EXISTS crm_email_templates_industry_role_trigger_key
        """)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'crm_email_templates_irtv_key'
                ) THEN
                    ALTER TABLE crm_email_templates
                    ADD CONSTRAINT crm_email_templates_irtv_key
                    UNIQUE (industry, role, trigger, variant_label);
                END IF;
            END $$;
        """)
        # Send-level log so we can attribute replies back to the
        # specific variant. One row per attempted send.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crm_email_sends (
                id              SERIAL PRIMARY KEY,
                contact_id      INTEGER NOT NULL REFERENCES crm_contacts(id) ON DELETE CASCADE,
                template_id     INTEGER REFERENCES crm_email_templates(id) ON DELETE SET NULL,
                sent_at         TIMESTAMP DEFAULT NOW(),
                replied         BOOLEAN NOT NULL DEFAULT FALSE,
                replied_at      TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS crm_email_sends_contact_idx
            ON crm_email_sends(contact_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS crm_email_sends_template_idx
            ON crm_email_sends(template_id)
        """)
        # Role enum on contact + templates. Templates can now be keyed
        # on (industry, role, trigger) — empty role acts as "any role"
        # fallback. Drop the old (industry, trigger) unique constraint
        # and replace with a 3-column one. Idempotent.
        cur.execute("""
            ALTER TABLE crm_contacts
            ADD COLUMN IF NOT EXISTS role VARCHAR(40)
        """)
        cur.execute("""
            ALTER TABLE crm_contacts
            ADD COLUMN IF NOT EXISTS hosting_model VARCHAR(32) DEFAULT 'TBD'
        """)
        cur.execute("""
            ALTER TABLE crm_contacts
            ADD COLUMN IF NOT EXISTS engagement_notes TEXT
        """)
        # Pilot Agreement Checklist — stored as JSON text so we can
        # iterate the schema without a migration each time.
        cur.execute("""
            ALTER TABLE crm_contacts
            ADD COLUMN IF NOT EXISTS pilot_agreement TEXT
        """)
        cur.execute("""
            ALTER TABLE crm_email_templates
            ADD COLUMN IF NOT EXISTS role VARCHAR(40) NOT NULL DEFAULT ''
        """)
        # Prototype tracking — one row per testable build per contact.
        # Captures the deployed URL the client can hit, the current
        # status, and an accumulating feedback log Aaron pastes back
        # from email replies.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crm_prototypes (
                id              SERIAL PRIMARY KEY,
                contact_id      INTEGER REFERENCES crm_contacts(id) ON DELETE CASCADE,
                name            VARCHAR(160) NOT NULL,
                prototype_url   TEXT,
                status          VARCHAR(32) NOT NULL DEFAULT 'BUILDING',
                description     TEXT,
                feedback        TEXT,
                notes           TEXT,
                created_at      TIMESTAMP DEFAULT NOW(),
                updated_at      TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS crm_prototypes_contact_idx
            ON crm_prototypes(contact_id)
        """)
        # Feedback token — random URL-safe string the client uses to
        # submit feedback without auth (the token IS the auth). Added
        # post-launch so guard with ADD COLUMN IF NOT EXISTS.
        cur.execute("""
            ALTER TABLE crm_prototypes
            ADD COLUMN IF NOT EXISTS feedback_token VARCHAR(64)
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS crm_prototypes_feedback_token_idx
            ON crm_prototypes(feedback_token)
            WHERE feedback_token IS NOT NULL
        """)
        # A/B testing needs UNIQUE(industry, role, trigger, variant_label)
        # — that's crm_email_templates_irtv_key, created above, and what
        # upsert_template's ON CONFLICT targets. Two older constraints
        # CONTRADICT it and must be dropped: the 2-column
        # (industry, trigger) and the 3-column (industry, role, trigger).
        # The 3-column one caps each (industry, role, trigger) at a single
        # variant (breaking the bandit) AND makes every re-seed of an
        # existing template raise a duplicate-key warning at startup.
        # Idempotent — DROP IF EXISTS is a no-op once they're gone.
        cur.execute("""
            ALTER TABLE crm_email_templates
            DROP CONSTRAINT IF EXISTS crm_email_templates_industry_trigger_key
        """)
        cur.execute("""
            ALTER TABLE crm_email_templates
            DROP CONSTRAINT IF EXISTS crm_email_templates_industry_role_trigger_key
        """)
        # Discovery-call artifacts — one row per contact (UNIQUE on
        # contact_id, upserted). Stores raw transcript + the AI chain
        # outputs + a derived scorecard. Multiple calls per contact
        # may come in v2; for now we overwrite the latest.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crm_discovery_calls (
                id              SERIAL PRIMARY KEY,
                contact_id      INTEGER NOT NULL UNIQUE
                                  REFERENCES crm_contacts(id) ON DELETE CASCADE,
                call_date       DATE,
                transcript      TEXT,
                extraction_json TEXT,
                exec_summary    TEXT,
                pain_analysis   TEXT,
                mvp_scope       TEXT,
                scorecard_json  TEXT,
                suggested_stage VARCHAR(32),
                created_at      TIMESTAMP DEFAULT NOW(),
                updated_at      TIMESTAMP DEFAULT NOW()
            )
        """)
        # Working-session artifacts — the 30-min pressure-test that
        # happens between DISCOVERY_CALL and PILOT. Same shape as
        # discovery calls but different prompts and a different
        # scorecard band map (≥85 = send proposal, etc.).
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crm_working_sessions (
                id                SERIAL PRIMARY KEY,
                contact_id        INTEGER NOT NULL UNIQUE
                                    REFERENCES crm_contacts(id) ON DELETE CASCADE,
                session_date      DATE,
                transcript        TEXT,
                extraction_json   TEXT,
                locked_scope      TEXT,
                success_criteria  TEXT,
                proposal_draft    TEXT,
                scorecard_json    TEXT,
                suggested_action  VARCHAR(40),
                created_at        TIMESTAMP DEFAULT NOW(),
                updated_at        TIMESTAMP DEFAULT NOW()
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


# ─── Sign-up list (Phase 1 of paywall) ─────────────────────────────────
# Email capture so we can DM people when paid features launch. No auth,
# no password — Phase 2 adds magic-link login when we actually need to
# gate features per user. add_user is idempotent on email (returns
# False if the email is already registered).

def add_user(email, name=None, source=None, user_agent=None):
    """Insert a signup. Returns (created: bool, user_id: int|None).
    On duplicate email, returns (False, existing_id)."""
    conn = _get_conn()
    if not conn: return (False, None)
    try:
        cur = conn.cursor()
        # ON CONFLICT DO NOTHING leaves the existing row alone, so a
        # re-signup is harmless. RETURNING id only fires when a row was
        # actually inserted; we look up the existing id otherwise.
        cur.execute(
            """INSERT INTO users (email, name, source, user_agent)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (email) DO NOTHING
               RETURNING id""",
            (email, name, source, user_agent),
        )
        row = cur.fetchone()
        if row:
            uid = row[0]
            conn.commit()
            return (True, uid)
        # Existed already — fetch the id so admin tooling can correlate.
        cur.execute("SELECT id FROM users WHERE email=%s", (email,))
        existing = cur.fetchone()
        return (False, existing[0] if existing else None)
    except Exception as e:
        logger.error(f"add_user failed: {e}")
        return (False, None)
    finally:
        # The success path already committed above; don't commit here —
        # committing an errored transaction is misleading, and `cur` may be
        # unbound if conn.cursor() itself raised.
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def get_user_count():
    conn = _get_conn()
    if not conn: return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        n = cur.fetchone()[0]
        cur.close(); conn.close()
        return n
    except Exception as e:
        logger.error(f"get_user_count: {e}")
        if conn: conn.close()
        return 0


def list_users(limit=500):
    """Returns recent N signups as a list of dicts. Used by /admin/signups."""
    conn = _get_conn()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, email, name, source, user_agent, created_at
               FROM users ORDER BY created_at DESC LIMIT %s""",
            (limit,),
        )
        rows = cur.fetchall()
        cur.close(); conn.close()
        return [{
            "id": r[0], "email": r[1], "name": r[2],
            "source": r[3], "user_agent": r[4],
            "created_at": r[5].isoformat() if r[5] else None,
        } for r in rows]
    except Exception as e:
        logger.error(f"list_users: {e}")
        if conn: conn.close()
        return []


# ─── Landscaper shared book ─────────────────────────────────────────
def landscaper_create_book(name: str) -> str | None:
    """Create a book with an unguessable code. Returns the code."""
    import secrets
    code = secrets.token_urlsafe(9)[:12]
    conn = _get_conn()
    if not conn: return None
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO landscaper_books (code, name) VALUES (%s, %s)",
                    (code, name[:80]))
        conn.commit(); cur.close()
        return code
    except Exception as e:
        logger.error(f"landscaper_create_book failed: {e}")
        return None
    finally:
        conn.close()


def landscaper_book_exists(code: str) -> bool:
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM landscaper_books WHERE code = %s", (code,))
        ok = cur.fetchone() is not None
        cur.close()
        return ok
    finally:
        conn.close()


def landscaper_list_books() -> list[dict]:
    conn = _get_conn()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("""SELECT b.code, b.name, b.created_at,
                              (SELECT COUNT(*) FROM landscaper_clients c
                               WHERE c.book_code = b.code)
                       FROM landscaper_books b ORDER BY b.created_at DESC""")
        rows = cur.fetchall(); cur.close()
        return [{"code": r[0], "name": r[1],
                 "created_at": r[2].isoformat() if r[2] else None,
                 "clients": r[3]} for r in rows]
    finally:
        conn.close()


def landscaper_get_book(code: str) -> dict | None:
    """{clients: [...], costs: {...}} or None if the code is unknown."""
    import json as _json
    conn = _get_conn()
    if not conn: return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT costs FROM landscaper_books WHERE code = %s", (code,))
        row = cur.fetchone()
        if row is None:
            cur.close()
            return None
        costs = {}
        if row[0]:
            try: costs = _json.loads(row[0])
            except ValueError: costs = {}
        cur.execute("""SELECT id, name, zip, price, freq FROM landscaper_clients
                       WHERE book_code = %s ORDER BY id""", (code,))
        clients = [{"id": r[0], "name": r[1], "zip": r[2],
                    "price": r[3], "freq": r[4]} for r in cur.fetchall()]
        cur.close()
        return {"clients": clients, "costs": costs}
    finally:
        conn.close()


def landscaper_add_client(code: str, name: str, zip_code: str,
                          price: float, freq: str) -> int | None:
    conn = _get_conn()
    if not conn: return None
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO landscaper_clients (book_code, name, zip, price, freq)
                       VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                    (code, name[:80], zip_code[:5], price, freq[:2]))
        cid = cur.fetchone()[0]
        conn.commit(); cur.close()
        return cid
    except Exception as e:
        logger.error(f"landscaper_add_client failed: {e}")
        return None
    finally:
        conn.close()


def landscaper_delete_client(code: str, client_id: int) -> bool:
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM landscaper_clients WHERE book_code = %s AND id = %s",
                    (code, client_id))
        n = cur.rowcount
        conn.commit(); cur.close()
        return n > 0
    finally:
        conn.close()


def landscaper_save_costs(code: str, costs: dict) -> bool:
    import json as _json
    conn = _get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("UPDATE landscaper_books SET costs = %s WHERE code = %s",
                    (_json.dumps(costs), code))
        n = cur.rowcount
        conn.commit(); cur.close()
        return n > 0
    finally:
        conn.close()
