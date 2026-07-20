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
    opensea_fee_bps     INTEGER DEFAULT 100,
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
CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades(wallet_address, block_timestamp);

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

CREATE TABLE IF NOT EXISTS flags (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT NOT NULL,
    color          TEXT NOT NULL DEFAULT '#58a6ff',
    is_auto        INTEGER NOT NULL DEFAULT 0,
    condition_json TEXT
);

CREATE TABLE IF NOT EXISTS wallet_flags (
    wallet_address TEXT NOT NULL,
    flag_id        INTEGER NOT NULL,
    PRIMARY KEY (wallet_address, flag_id),
    FOREIGN KEY (flag_id) REFERENCES flags(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_wallet_flags_addr ON wallet_flags(wallet_address);

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

CREATE TABLE IF NOT EXISTS watchlist (
    slug             TEXT PRIMARY KEY,
    name             TEXT,
    contract_address TEXT,
    added_at         INTEGER
);

CREATE TABLE IF NOT EXISTS market_trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    slug            TEXT NOT NULL,
    tx_hash         TEXT NOT NULL,
    block_timestamp INTEGER NOT NULL,
    eth_amount      REAL NOT NULL,
    buyer_address   TEXT,
    seller_address  TEXT,
    nft_id          TEXT,
    UNIQUE(slug, tx_hash, nft_id)
);

CREATE INDEX IF NOT EXISTS idx_market_trades_slug_ts ON market_trades(slug, block_timestamp);

CREATE TABLE IF NOT EXISTS floor_history (
    slug           TEXT NOT NULL,
    ts             INTEGER NOT NULL,
    floor_eth      REAL,
    best_offer_eth REAL
);

CREATE INDEX IF NOT EXISTS idx_floor_history_slug_ts ON floor_history(slug, ts);

CREATE TABLE IF NOT EXISTS market_sync_state (
    slug           TEXT PRIMARY KEY,
    last_event_ts  INTEGER DEFAULT 0,
    last_synced_at INTEGER
);

CREATE TABLE IF NOT EXISTS sales (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_slug TEXT    NOT NULL,
    tx_hash         TEXT    NOT NULL,
    nft_id          TEXT    NOT NULL,
    timestamp       INTEGER NOT NULL,
    price_eth       REAL    NOT NULL,
    payment_token   TEXT,
    sale_type       TEXT,
    seller          TEXT,
    buyer           TEXT,
    UNIQUE(tx_hash, nft_id)
);

CREATE INDEX IF NOT EXISTS idx_sales_slug_ts ON sales(collection_slug, timestamp);

CREATE TABLE IF NOT EXISTS collection_sync_state (
    collection_slug     TEXT PRIMARY KEY,
    oldest_ts_fetched   INTEGER,
    last_synced_at      INTEGER
);

CREATE TABLE IF NOT EXISTS entities (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    notes       TEXT,
    created_at  INTEGER
);

CREATE TABLE IF NOT EXISTS entity_summaries (
    entity_id           INTEGER PRIMARY KEY,
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
    collections_traded  INTEGER,
    wash_legs           INTEGER DEFAULT 0,
    wash_cost_eth       REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dashboard_users (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at INTEGER
);

CREATE TABLE IF NOT EXISTS user_checkpoints (
    user_id         TEXT,
    view_key        TEXT,
    last_checked_at INTEGER,
    PRIMARY KEY (user_id, view_key)
);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


_MARKET_SEED = [
    ("cryptopunks",                 "CryptoPunks"),
    ("chimpersnft",                 "Chimpers"),
    ("good-vibes-club",             "Good Vibes Club"),
    ("boredapeyachtclub",           "Bored Ape Yacht Club"),
    ("pudgypenguins",               "Pudgy Penguins"),
    ("quirkiesoriginals",           "Quirkies Originals"),
    ("max-pain-and-frens-by-xcopy", "MAX PAIN AND FRENS"),
    ("vv-checks-originals",         "Checks - VV Originals"),
    ("moonbirds",                   "Moonbirds"),
    ("otherdeed-expanded",          "Otherdeed Expanded"),
    ("meebits",                     "Meebits"),
    ("cryptoadz-by-gremplin",       "CrypToadz by GREMPLIN"),
    ("nakamigos",                   "Nakamigos"),
    ("goblintownwtf",               "goblintown.wtf"),
    ("cryptodickbutts-s3",          "CryptoDickbutts"),
    ("chromie-squiggle-by-snowfro", "Chromie Squiggle by Snowfro"),
    ("glhfers",                     "GLHFers"),
    ("genuine-undead",              "Genuine Undead"),
]


def init_db():
    import time as _time
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        watchlist_existed = bool(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='watchlist'"
        ).fetchone())
        for stmt in (s.strip() for s in SCHEMA.split(";") if s.strip()):
            conn.execute(stmt)
        conn.commit()
        for col_def in [
            "ALTER TABLE collections ADD COLUMN floor_price_eth REAL",
            "ALTER TABLE collections ADD COLUMN best_offer_eth REAL",
            "ALTER TABLE collections ADD COLUMN floor_fetched_at INTEGER",
            "ALTER TABLE trades ADD COLUMN sell_type TEXT",
            "ALTER TABLE market_trades ADD COLUMN sell_type TEXT",
            "ALTER TABLE collections ADD COLUMN avg_gross_spread_eth REAL",
            "ALTER TABLE collections ADD COLUMN avg_net_spread_eth REAL",
            "ALTER TABLE collections ADD COLUMN avg_gross_spread_pct REAL",
            "ALTER TABLE collections ADD COLUMN avg_net_spread_pct REAL",
            "ALTER TABLE collections ADD COLUMN spread_pair_count INTEGER",
            "ALTER TABLE collections ADD COLUMN spread_updated_at INTEGER",
            "ALTER TABLE collections ADD COLUMN avg_daily_sales_alltime REAL",
            "ALTER TABLE collections ADD COLUMN avg_daily_sales_30d REAL",
            "ALTER TABLE collections ADD COLUMN total_trades INTEGER",
            "ALTER TABLE wallets ADD COLUMN entity_id INTEGER",
        ]:
            try:
                conn.execute(col_def)
            except sqlite3.OperationalError:
                pass
        if not watchlist_existed:
            now = int(_time.time())
            for slug, name in _MARKET_SEED:
                conn.execute(
                    "INSERT OR IGNORE INTO watchlist (slug, name, contract_address, added_at) VALUES (?,?,?,?)",
                    (slug, name, "", now),
                )
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


# ---------- flags ----------

def list_flags(conn) -> list:
    return conn.execute("SELECT * FROM flags ORDER BY name").fetchall()


def upsert_flag(conn, flag_id, name: str, color: str,
                is_auto: int = 0, condition_json: str = None) -> int:
    if flag_id:
        conn.execute(
            "UPDATE flags SET name=?, color=?, is_auto=?, condition_json=? WHERE id=?",
            (name, color, is_auto, condition_json, flag_id),
        )
        return flag_id
    cur = conn.execute(
        "INSERT INTO flags (name, color, is_auto, condition_json) VALUES (?,?,?,?)",
        (name, color, is_auto, condition_json),
    )
    return cur.lastrowid


def delete_flag(conn, flag_id: int):
    conn.execute("DELETE FROM wallet_flags WHERE flag_id = ?", (flag_id,))
    conn.execute("DELETE FROM flags WHERE id = ?", (flag_id,))


def get_flags_for_wallets(conn, addresses: list) -> dict:
    """Returns {address_lower: [flag_dict, ...]}. Single JOIN query."""
    if not addresses:
        return {}
    ph = ",".join("?" * len(addresses))
    rows = conn.execute(
        f"SELECT wf.wallet_address, f.id, f.name, f.color, f.is_auto "
        f"FROM wallet_flags wf JOIN flags f ON wf.flag_id = f.id "
        f"WHERE wf.wallet_address IN ({ph}) ORDER BY wf.wallet_address, f.name",
        [a.lower() for a in addresses],
    ).fetchall()
    result = {a.lower(): [] for a in addresses}
    for row in rows:
        result[row["wallet_address"]].append(dict(row))
    return result


def assign_flag(conn, address: str, flag_id: int):
    conn.execute(
        "INSERT OR IGNORE INTO wallet_flags (wallet_address, flag_id) VALUES (?,?)",
        (address.lower(), flag_id),
    )


def unassign_flag(conn, address: str, flag_id: int):
    conn.execute(
        "DELETE FROM wallet_flags WHERE wallet_address=? AND flag_id=?",
        (address.lower(), flag_id),
    )


def apply_auto_flags_for_wallet(conn, address: str):
    """Re-evaluates all is_auto=1 flags against wallet_summaries. Safe when no summary exists."""
    import json as _json
    summary = conn.execute(
        "SELECT * FROM wallet_summaries WHERE wallet_address = ?",
        (address.lower(),),
    ).fetchone()
    auto_flags = conn.execute("SELECT * FROM flags WHERE is_auto = 1").fetchall()
    ALLOWED_FIELDS = {
        "total_trades", "realized_pnl_eth", "win_rate",
        "open_positions", "collections_traded", "total_buy_eth",
    }
    for flag in auto_flags:
        cond_str = flag["condition_json"]
        if not cond_str:
            continue
        try:
            cond = _json.loads(cond_str)
            field = cond.get("field")
            op    = cond.get("op")
            value = cond.get("value")
            if not field or not op or value is None or field not in ALLOWED_FIELDS:
                continue
            actual = summary[field] if summary else None
            if actual is None:
                conn.execute(
                    "DELETE FROM wallet_flags WHERE wallet_address=? AND flag_id=?",
                    (address.lower(), flag["id"]),
                )
                continue
            match = {
                ">":  actual >  value,
                "<":  actual <  value,
                ">=": actual >= value,
                "<=": actual <= value,
                "=":  actual == value,
                "!=": actual != value,
            }.get(op, False)
            if match:
                conn.execute(
                    "INSERT OR IGNORE INTO wallet_flags (wallet_address, flag_id) VALUES (?,?)",
                    (address.lower(), flag["id"]),
                )
            else:
                conn.execute(
                    "DELETE FROM wallet_flags WHERE wallet_address=? AND flag_id=?",
                    (address.lower(), flag["id"]),
                )
        except Exception:
            continue


def apply_auto_name_if_blank(conn, address: str):
    """Sets 'Vol_<trades_24m>_<yyyy-mm-dd>' on wallets with no name."""
    import time as _time
    from datetime import datetime, timezone
    wallet = get_wallet(conn, address.lower())
    if not wallet or wallet["name"]:
        return
    cutoff = int(_time.time()) - 730 * 86400
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM trades WHERE wallet_address=? AND block_timestamp>=?",
        (address.lower(), cutoff),
    ).fetchone()
    trade_count = row["cnt"] if row else 0
    created_ts = wallet["created_at"] or int(_time.time())
    import_date = datetime.fromtimestamp(created_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    conn.execute(
        "UPDATE wallets SET name=? WHERE address=? AND (name IS NULL OR name='')",
        (f"Vol_{trade_count}_{import_date}", address.lower()),
    )


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


# ---------- market watchlist ----------

def list_watchlist(conn) -> list:
    return conn.execute("SELECT * FROM watchlist ORDER BY name").fetchall()


def add_watchlist(conn, slug: str, name: str, contract_address: str = ""):
    import time as _time
    conn.execute(
        "INSERT INTO watchlist (slug, name, contract_address, added_at) VALUES (?,?,?,?) "
        "ON CONFLICT(slug) DO UPDATE SET name=excluded.name, contract_address=excluded.contract_address",
        (slug, name, contract_address or "", int(_time.time())),
    )


def remove_watchlist(conn, slug: str):
    conn.execute("DELETE FROM watchlist WHERE slug = ?", (slug,))


# ---------- market trades ----------

def insert_market_trade(conn, row: dict) -> bool:
    try:
        conn.execute(
            "INSERT INTO market_trades (slug, tx_hash, block_timestamp, eth_amount, "
            "buyer_address, seller_address, nft_id, sell_type) VALUES (?,?,?,?,?,?,?,?)",
            (row["slug"], row["tx_hash"], row["block_timestamp"], row["eth_amount"],
             row.get("buyer_address", ""), row.get("seller_address", ""), row.get("nft_id", ""),
             row.get("sell_type")),
        )
        return True
    except sqlite3.IntegrityError:
        return False


# ---------- floor history ----------

def insert_floor_snapshot(conn, slug: str, floor_eth, offer_eth, ts: int):
    conn.execute(
        "INSERT INTO floor_history (slug, ts, floor_eth, best_offer_eth) VALUES (?,?,?,?)",
        (slug, ts, floor_eth, offer_eth),
    )


def get_latest_floor_snapshot(conn, slug: str):
    return conn.execute(
        "SELECT * FROM floor_history WHERE slug=? ORDER BY ts DESC LIMIT 1", (slug,)
    ).fetchone()


def get_floor_at(conn, slug: str, cutoff_ts: int):
    row = conn.execute(
        "SELECT * FROM floor_history WHERE slug=? AND ts<=? ORDER BY ts DESC LIMIT 1",
        (slug, cutoff_ts),
    ).fetchone()
    if row:
        return row
    return conn.execute(
        "SELECT * FROM floor_history WHERE slug=? ORDER BY ts ASC LIMIT 1", (slug,)
    ).fetchone()


# ---------- collection trading data (sales / spread, fed by update_collections.py) ----------

def upsert_collection_market(conn, collection: dict, prices: dict):
    """Upsert a sister-pipeline collection sweep, keyed by contract_address.

    Floor/offer use COALESCE so a sweep that failed to fetch a price doesn't
    null out a value another code path cached.
    """
    import time as _time
    ca = (collection.get("contract_address") or "").strip().lower()
    if not ca:
        return False
    now = int(_time.time())
    floor = prices.get("floor")
    conn.execute("""
        INSERT INTO collections
            (contract_address, slug, name, creator_fee_bps, opensea_fee_bps,
             total_fee_bps, floor_price_eth, best_offer_eth, fetched_at, floor_fetched_at)
        VALUES (:ca, :slug, :name, :creator_fee_bps, :opensea_fee_bps,
                :total_fee_bps, :floor, :best_offer, :now, :floor_fetched_at)
        ON CONFLICT(contract_address) DO UPDATE SET
            slug             = excluded.slug,
            name             = excluded.name,
            creator_fee_bps  = excluded.creator_fee_bps,
            opensea_fee_bps  = excluded.opensea_fee_bps,
            total_fee_bps    = excluded.total_fee_bps,
            floor_price_eth  = COALESCE(excluded.floor_price_eth, floor_price_eth),
            best_offer_eth   = COALESCE(excluded.best_offer_eth, best_offer_eth),
            fetched_at       = excluded.fetched_at,
            floor_fetched_at = COALESCE(excluded.floor_fetched_at, floor_fetched_at)
    """, {
        "ca": ca,
        "slug": collection["slug"],
        "name": collection.get("name"),
        "creator_fee_bps": collection.get("creator_fee_bps"),
        "opensea_fee_bps": collection.get("opensea_fee_bps"),
        "total_fee_bps": collection.get("total_fee_bps"),
        "floor": floor,
        "best_offer": prices.get("best_offer"),
        "now": now,
        "floor_fetched_at": now if floor is not None else None,
    })
    conn.commit()
    return True


def update_collection_spread(conn, contract_address: str, spread: dict):
    """Persist computed daily-avg spread + volume stats for a collection."""
    import time as _time
    conn.execute("""
        UPDATE collections
        SET avg_gross_spread_eth    = :avg_gross_spread_eth,
            avg_net_spread_eth      = :avg_net_spread_eth,
            avg_gross_spread_pct    = :avg_gross_spread_pct,
            avg_net_spread_pct      = :avg_net_spread_pct,
            spread_pair_count       = :pair_count,
            spread_updated_at       = :updated_at,
            avg_daily_sales_alltime = :avg_daily_sales_alltime,
            avg_daily_sales_30d     = :avg_daily_sales_30d,
            total_trades            = :total_trades
        WHERE contract_address = :ca
    """, {**spread, "ca": contract_address.lower(), "updated_at": int(_time.time())})
    conn.commit()


def insert_sales(conn, slug: str, events: list) -> int:
    """Insert sale events, skipping duplicates. Returns number of new rows."""
    rows = [
        (slug, e["tx_hash"], e["nft_id"], e["timestamp"], e["price_eth"],
         e["payment_token"], e["sale_type"], e["seller"], e["buyer"])
        for e in events
    ]
    cur = conn.executemany("""
        INSERT OR IGNORE INTO sales
            (collection_slug, tx_hash, nft_id, timestamp, price_eth,
             payment_token, sale_type, seller, buyer)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    return cur.rowcount


def get_sales(conn, slug: str, since_ts: int = 0) -> list:
    rows = conn.execute("""
        SELECT nft_id, tx_hash, timestamp, price_eth, payment_token, sale_type, seller, buyer
        FROM sales
        WHERE collection_slug = ? AND timestamp >= ?
        ORDER BY timestamp DESC
    """, (slug, since_ts)).fetchall()
    return [dict(r) for r in rows]


def get_coldata_sync_state(conn, slug: str):
    row = conn.execute(
        "SELECT oldest_ts_fetched, last_synced_at FROM collection_sync_state WHERE collection_slug = ?",
        (slug,)
    ).fetchone()
    return dict(row) if row else None


def update_coldata_sync_state(conn, slug: str, oldest_ts: int):
    import time as _time
    conn.execute("""
        INSERT INTO collection_sync_state (collection_slug, oldest_ts_fetched, last_synced_at)
        VALUES (?, ?, ?)
        ON CONFLICT(collection_slug) DO UPDATE SET
            oldest_ts_fetched = MIN(oldest_ts_fetched, excluded.oldest_ts_fetched),
            last_synced_at    = excluded.last_synced_at
    """, (slug, oldest_ts, int(_time.time())))
    conn.commit()


def list_coldata_slugs(conn) -> list:
    return [r["collection_slug"] for r in conn.execute(
        "SELECT collection_slug FROM collection_sync_state ORDER BY collection_slug"
    ).fetchall()]


# ---------- market sync state ----------

def get_market_sync_state(conn, slug: str):
    return conn.execute(
        "SELECT * FROM market_sync_state WHERE slug=?", (slug,)
    ).fetchone()


def set_market_sync_state(conn, slug: str, last_event_ts: int, last_synced_at: int):
    conn.execute(
        "INSERT INTO market_sync_state (slug, last_event_ts, last_synced_at) VALUES (?,?,?) "
        "ON CONFLICT(slug) DO UPDATE SET last_event_ts=excluded.last_event_ts, "
        "last_synced_at=excluded.last_synced_at",
        (slug, last_event_ts, last_synced_at),
    )


# ---------- entities ----------

def create_entity(conn, name: str, notes: str = None) -> int:
    import time as _time
    cur = conn.execute(
        "INSERT INTO entities (name, notes, created_at) VALUES (?, ?, ?)",
        (name, notes, int(_time.time()))
    )
    return cur.lastrowid


def get_entity(conn, entity_id: int):
    return conn.execute("SELECT * FROM entities WHERE id = ?", (entity_id,)).fetchone()


def update_entity(conn, entity_id: int, name: str = None, notes: str = None):
    conn.execute(
        "UPDATE entities SET name = COALESCE(?, name), notes = COALESCE(?, notes) WHERE id = ?",
        (name, notes, entity_id)
    )


def delete_entity(conn, entity_id: int):
    conn.execute("UPDATE wallets SET entity_id = NULL WHERE entity_id = ?", (entity_id,))
    conn.execute("DELETE FROM entity_summaries WHERE entity_id = ?", (entity_id,))
    conn.execute("DELETE FROM entities WHERE id = ?", (entity_id,))


def get_entity_members(conn, entity_id: int) -> list:
    rows = conn.execute(
        "SELECT address FROM wallets WHERE entity_id = ? ORDER BY address", (entity_id,)
    ).fetchall()
    return [r["address"] for r in rows]


def get_entity_member_rows(conn, entity_id: int) -> list:
    rows = conn.execute(
        "SELECT address, name FROM wallets WHERE entity_id = ? ORDER BY address", (entity_id,)
    ).fetchall()
    return [{"address": r["address"], "name": r["name"]} for r in rows]


def set_entity_members(conn, entity_id: int, addresses: list):
    conn.execute("UPDATE wallets SET entity_id = NULL WHERE entity_id = ?", (entity_id,))
    if addresses:
        ph = ",".join("?" * len(addresses))
        conn.execute(
            f"UPDATE wallets SET entity_id = ? WHERE address IN ({ph})",
            [entity_id] + [a.lower() for a in addresses]
        )


def get_trades_multi(conn, addresses: list, since: int = None) -> list:
    if not addresses:
        return []
    ph = ",".join("?" * len(addresses))
    params = [a.lower() for a in addresses]
    if since:
        return conn.execute(f"""
            SELECT t.*, c.name AS collection_name, c.total_fee_bps
            FROM trades t
            LEFT JOIN collections c ON t.collection_address = c.contract_address
            WHERE t.wallet_address IN ({ph}) AND t.block_timestamp >= ?
            ORDER BY t.block_timestamp ASC
        """, params + [since]).fetchall()
    return conn.execute(f"""
        SELECT t.*, c.name AS collection_name, c.total_fee_bps
        FROM trades t
        LEFT JOIN collections c ON t.collection_address = c.contract_address
        WHERE t.wallet_address IN ({ph})
        ORDER BY t.block_timestamp ASC
    """, params).fetchall()


def get_latest_trade_ts_multi(conn, addresses: list) -> int:
    if not addresses:
        return 0
    ph = ",".join("?" * len(addresses))
    row = conn.execute(
        f"SELECT MAX(block_timestamp) AS ts FROM trades WHERE wallet_address IN ({ph})",
        [a.lower() for a in addresses]
    ).fetchone()
    return row["ts"] or 0


def get_entity_summary(conn, entity_id: int):
    return conn.execute(
        "SELECT * FROM entity_summaries WHERE entity_id = ?", (entity_id,)
    ).fetchone()


def upsert_entity_summary(conn, entity_id: int, summary: dict, latest_trade_ts: int):
    import time as _time
    s = summary
    conn.execute("""
        INSERT INTO entity_summaries
            (entity_id, computed_at, latest_trade_ts, total_trades, total_buys, total_sells,
             unmatched_sells, total_buy_eth, total_sell_eth, total_fees_eth, total_gas_eth,
             realized_pnl_eth, win_rate, avg_holding_secs, open_positions, open_cost_basis_eth,
             collections_traded, wash_legs, wash_cost_eth)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(entity_id) DO UPDATE SET
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
            collections_traded  = excluded.collections_traded,
            wash_legs           = excluded.wash_legs,
            wash_cost_eth       = excluded.wash_cost_eth
    """, (
        entity_id, int(_time.time()), latest_trade_ts,
        s["total_trades"], s["total_buys"], s["total_sells"],
        s["unmatched_sells"],
        s["total_buy_eth"], s["total_sell_eth"], s["total_fees_eth"], s["total_gas_eth"],
        s["realized_pnl_eth"], s["win_rate"], s.get("avg_holding") or 0,
        s["open_positions"], s["open_cost_basis_eth"], s["collections_traded"],
        s.get("wash_legs", 0), s.get("wash_cost_eth", 0.0),
    ))


# ---------- dashboard users ----------

def list_dashboard_users(conn):
    return conn.execute("SELECT id, name FROM dashboard_users ORDER BY created_at").fetchall()


def create_dashboard_user(conn, user_id, name):
    import time as _time
    conn.execute(
        "INSERT INTO dashboard_users (id, name, created_at) VALUES (?, ?, ?)",
        (user_id, name, int(_time.time()))
    )


def get_checkpoint(conn, user_id, view_key):
    row = conn.execute(
        "SELECT last_checked_at FROM user_checkpoints WHERE user_id=? AND view_key=?",
        (user_id, view_key)
    ).fetchone()
    return row["last_checked_at"] if row else None


def set_checkpoint(conn, user_id, view_key, ts):
    conn.execute(
        "INSERT INTO user_checkpoints (user_id, view_key, last_checked_at) VALUES (?, ?, ?) "
        "ON CONFLICT(user_id, view_key) DO UPDATE SET last_checked_at=excluded.last_checked_at",
        (user_id, view_key, ts)
    )
