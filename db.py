"""Database setup and access for NFT trade tracking."""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "nft_trades.db")

FLOOR_CACHE_TTL_SECS = 7 * 24 * 3600  # 604800

SCHEMA = """
CREATE TABLE IF NOT EXISTS collections (
    contract_address    TEXT PRIMARY KEY,
    slug                TEXT,
    name                TEXT,
    creator_fee_bps     INTEGER DEFAULT 0,
    opensea_fee_bps     INTEGER DEFAULT 250,
    total_fee_bps       INTEGER DEFAULT 0,
    fetched_at          INTEGER,
    floor_price_eth     REAL,
    best_offer_eth      REAL,
    floor_fetched_at    INTEGER
);

CREATE TABLE IF NOT EXISTS trades (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address      TEXT NOT NULL,
    tx_hash             TEXT NOT NULL,
    block_timestamp     INTEGER NOT NULL,
    side                TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
    eth_amount          REAL NOT NULL,
    gas_eth             REAL DEFAULT 0.0,
    buyer_address       TEXT NOT NULL,
    seller_address      TEXT NOT NULL,
    collection_address  TEXT NOT NULL,
    collection_slug     TEXT,
    nft_id              TEXT NOT NULL,
    marketplace         TEXT DEFAULT 'opensea',
    sell_type           TEXT,
    UNIQUE(tx_hash, wallet_address, nft_id, side)
);

CREATE INDEX IF NOT EXISTS idx_trades_wallet    ON trades(wallet_address);
CREATE INDEX IF NOT EXISTS idx_trades_collection ON trades(collection_address);
CREATE INDEX IF NOT EXISTS idx_trades_nft       ON trades(collection_address, nft_id);

CREATE TABLE IF NOT EXISTS sync_state (
    wallet_address      TEXT PRIMARY KEY,
    last_synced_at      INTEGER,
    last_cursor         TEXT,
    full_sync_complete  INTEGER DEFAULT 0,
    total_inserted      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS wallets (
    address     TEXT PRIMARY KEY,
    name        TEXT,
    notes       TEXT,
    created_at  INTEGER
);

CREATE TABLE IF NOT EXISTS wallet_summaries (
    wallet_address      TEXT PRIMARY KEY,
    computed_at         INTEGER,
    latest_trade_ts     INTEGER,
    total_trades        INTEGER,
    total_buys          INTEGER,
    total_sells         INTEGER,
    unmatched_sells     INTEGER,
    total_buy_eth       REAL,
    total_sell_eth      REAL,
    total_fees_eth      REAL,
    total_gas_eth       REAL,
    realized_pnl_eth    REAL,
    win_rate            REAL,
    avg_holding_secs    REAL,
    open_positions      INTEGER,
    open_cost_basis_eth REAL,
    collections_traded  INTEGER
);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        for stmt in (s.strip() for s in SCHEMA.split(";") if s.strip()):
            conn.execute(stmt)
        conn.commit()
        for col_def in [
            "ALTER TABLE collections ADD COLUMN floor_price_eth REAL",
            "ALTER TABLE collections ADD COLUMN best_offer_eth REAL",
            "ALTER TABLE collections ADD COLUMN floor_fetched_at INTEGER",
            "ALTER TABLE trades ADD COLUMN sell_type TEXT",
        ]:
            try:
                conn.execute(col_def)
            except sqlite3.OperationalError:
                pass
        conn.commit()
    finally:
        conn.close()


# ---------- collections ----------

def upsert_collection(conn, contract_address: str, slug: str, name: str,
                      creator_fee_bps: int, opensea_fee_bps: int, fetched_at: int):
    total = creator_fee_bps + opensea_fee_bps
    conn.execute("""
        INSERT INTO collections (contract_address, slug, name, creator_fee_bps, opensea_fee_bps, total_fee_bps, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(contract_address) DO UPDATE SET
            slug = excluded.slug,
            name = excluded.name,
            creator_fee_bps = excluded.creator_fee_bps,
            opensea_fee_bps = excluded.opensea_fee_bps,
            total_fee_bps = excluded.total_fee_bps,
            fetched_at = excluded.fetched_at
    """, (contract_address, slug, name, creator_fee_bps, opensea_fee_bps, total, fetched_at))


def get_collection(conn, contract_address: str):
    return conn.execute(
        "SELECT * FROM collections WHERE contract_address = ?", (contract_address,)
    ).fetchone()


def get_cached_floors(conn, slugs: list) -> dict:
    if not slugs:
        return {}
    placeholders = ",".join("?" * len(slugs))
    rows = conn.execute(
        f"SELECT slug, floor_price_eth, best_offer_eth, floor_fetched_at "
        f"FROM collections WHERE slug IN ({placeholders})",
        slugs
    ).fetchall()
    return {row["slug"]: dict(row) for row in rows}


def upsert_collection_floor(conn, slug: str, floor_eth, offer_eth, now: int):
    conn.execute(
        "UPDATE collections SET floor_price_eth=?, best_offer_eth=?, floor_fetched_at=? WHERE slug=?",
        (floor_eth, offer_eth, now, slug)
    )


# ---------- trades ----------

def insert_trade(conn, trade: dict) -> bool:
    """Returns True if inserted (not a duplicate)."""
    try:
        conn.execute("""
            INSERT INTO trades
                (wallet_address, tx_hash, block_timestamp, side, eth_amount, gas_eth,
                 buyer_address, seller_address, collection_address, collection_slug, nft_id, marketplace,
                 sell_type)
            VALUES
                (:wallet_address, :tx_hash, :block_timestamp, :side, :eth_amount, :gas_eth,
                 :buyer_address, :seller_address, :collection_address, :collection_slug, :nft_id, :marketplace,
                 :sell_type)
        """, {**trade, "sell_type": trade.get("sell_type")})
        return True
    except sqlite3.IntegrityError:
        # Backfill sell_type for existing rows that don't have it yet
        if trade.get("sell_type"):
            conn.execute("""
                UPDATE trades SET sell_type = ?
                WHERE tx_hash = ? AND wallet_address = ? AND nft_id = ? AND side = ?
                  AND sell_type IS NULL
            """, (trade["sell_type"], trade["tx_hash"], trade["wallet_address"],
                  trade["nft_id"], trade["side"]))
        return False


def get_trades(conn, wallet_address: str, since: int = None) -> list:
    if since:
        return conn.execute("""
            SELECT t.*, c.name AS collection_name, c.total_fee_bps
            FROM trades t
            LEFT JOIN collections c ON t.collection_address = c.contract_address
            WHERE t.wallet_address = ? AND t.block_timestamp >= ?
            ORDER BY t.block_timestamp ASC
        """, (wallet_address.lower(), since)).fetchall()
    return conn.execute("""
        SELECT t.*, c.name AS collection_name, c.total_fee_bps
        FROM trades t
        LEFT JOIN collections c ON t.collection_address = c.contract_address
        WHERE t.wallet_address = ?
        ORDER BY t.block_timestamp ASC
    """, (wallet_address.lower(),)).fetchall()


# ---------- sync state ----------

def get_sync_state(conn, wallet_address: str):
    return conn.execute(
        "SELECT * FROM sync_state WHERE wallet_address = ?", (wallet_address.lower(),)
    ).fetchone()


def set_sync_state(conn, wallet_address: str, last_synced_at: int,
                   last_cursor: str = None, full_sync_complete: bool = False,
                   total_inserted: int = 0):
    conn.execute("""
        INSERT INTO sync_state (wallet_address, last_synced_at, last_cursor, full_sync_complete, total_inserted)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(wallet_address) DO UPDATE SET
            last_synced_at     = excluded.last_synced_at,
            last_cursor        = excluded.last_cursor,
            full_sync_complete = excluded.full_sync_complete,
            total_inserted     = sync_state.total_inserted + excluded.total_inserted
    """, (wallet_address.lower(), last_synced_at, last_cursor,
          1 if full_sync_complete else 0, total_inserted))


# ---------- wallets ----------

def upsert_wallet(conn, address: str, name: str = None, notes: str = None):
    import time as _time
    conn.execute("""
        INSERT INTO wallets (address, name, notes, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            name = COALESCE(excluded.name, name),
            notes = COALESCE(excluded.notes, notes)
    """, (address.lower(), name, notes, int(_time.time())))


def get_wallet(conn, address: str):
    return conn.execute(
        "SELECT * FROM wallets WHERE address = ?", (address.lower(),)
    ).fetchone()


def list_wallets(conn) -> list:
    return conn.execute("""
        SELECT w.*, s.last_synced_at,
               ws.realized_pnl_eth, ws.total_trades, ws.computed_at
        FROM wallets w
        LEFT JOIN sync_state s   ON w.address = s.wallet_address
        LEFT JOIN wallet_summaries ws ON w.address = ws.wallet_address
        ORDER BY w.name
    """).fetchall()


# ---------- wallet summaries ----------

def get_latest_trade_ts(conn, wallet_address: str) -> int:
    row = conn.execute(
        "SELECT MAX(block_timestamp) AS ts FROM trades WHERE wallet_address = ?",
        (wallet_address.lower(),)
    ).fetchone()
    return row["ts"] or 0


def get_wallet_summary(conn, wallet_address: str):
    return conn.execute(
        "SELECT * FROM wallet_summaries WHERE wallet_address = ?",
        (wallet_address.lower(),)
    ).fetchone()


def upsert_wallet_summary(conn, wallet_address: str, summary: dict, latest_trade_ts: int):
    import time as _time
    s = summary
    conn.execute("""
        INSERT INTO wallet_summaries
            (wallet_address, computed_at, latest_trade_ts, total_trades, total_buys, total_sells,
             unmatched_sells, total_buy_eth, total_sell_eth, total_fees_eth, total_gas_eth,
             realized_pnl_eth, win_rate, avg_holding_secs, open_positions, open_cost_basis_eth,
             collections_traded)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(wallet_address) DO UPDATE SET
            computed_at         = excluded.computed_at,
            latest_trade_ts     = excluded.latest_trade_ts,
            total_trades        = excluded.total_trades,
            total_buys          = excluded.total_buys,
            total_sells         = excluded.total_sells,
            unmatched_sells     = excluded.unmatched_sells,
            total_buy_eth       = excluded.total_buy_eth,
            total_sell_eth      = excluded.total_sell_eth,
            total_fees_eth      = excluded.total_fees_eth,
            total_gas_eth       = excluded.total_gas_eth,
            realized_pnl_eth    = excluded.realized_pnl_eth,
            win_rate            = excluded.win_rate,
            avg_holding_secs    = excluded.avg_holding_secs,
            open_positions      = excluded.open_positions,
            open_cost_basis_eth = excluded.open_cost_basis_eth,
            collections_traded  = excluded.collections_traded
    """, (
        wallet_address.lower(), int(_time.time()), latest_trade_ts,
        s["total_trades"], s["total_buys"], s["total_sells"],
        s["unmatched_sells"],
        s["total_buy_eth"], s["total_sell_eth"], s["total_fees_eth"], s["total_gas_eth"],
        s["realized_pnl_eth"], s["win_rate"], s["avg_holding"],
        s["open_positions"], s["open_cost_basis_eth"], s["collections_traded"],
    ))
