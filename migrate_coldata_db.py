"""One-shot migration: fold the sister project's collection_trades.db into nft_trades.db.

After this runs, nft_trades.db is the single database serving both projects:
  - `collections` gains the sister's spread/stats columns and absorbs its rows
    (keyed by contract_address, the player-analysis convention)
  - `sales` (market-wide per-collection sale events) is created and copied over
  - the sister's `sync_state` is copied into a new `collection_sync_state` table
    (the name `sync_state` is already taken by the per-wallet table here)

Idempotent: safe to re-run. Stop both apps before running.

Usage:
    python migrate_coldata_db.py
    python migrate_coldata_db.py --source "..\\collection trading data\\collection_trades.db"
"""

import argparse
import os
import shutil
import sqlite3
import sys
import time

DEFAULT_SOURCE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "collection trading data", "collection_trades.db",
)
DEFAULT_TARGET = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nft_trades.db")

# Sister-project columns added to the existing collections table
SPREAD_COLUMNS = [
    ("avg_gross_spread_eth", "REAL"),
    ("avg_net_spread_eth", "REAL"),
    ("avg_gross_spread_pct", "REAL"),
    ("avg_net_spread_pct", "REAL"),
    ("spread_pair_count", "INTEGER"),
    ("spread_updated_at", "INTEGER"),
    ("avg_daily_sales_alltime", "REAL"),
    ("avg_daily_sales_30d", "REAL"),
    ("total_trades", "INTEGER"),
]

NEW_TABLES = """
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
"""


def backup(target: str) -> str:
    """Checkpoint the WAL so the .db file is complete, then copy it."""
    conn = sqlite3.connect(target, timeout=30)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    dest = f"{target}.bak-{time.strftime('%Y%m%d-%H%M%S')}"
    shutil.copy2(target, dest)
    return dest


def merge_collections(tgt: sqlite3.Connection) -> None:
    """Row-by-row upsert of src.collections into collections, keyed by contract_address.

    Policy: sister values win when non-null (its floor/fee sweep is the fresher
    source for market data); sister updated_at maps onto fetched_at/floor_fetched_at.
    """
    src_rows = tgt.execute("SELECT * FROM src.collections").fetchall()
    inserted = updated = 0
    skipped, conflicts = [], []

    for r in src_rows:
        ca = (r["contract_address"] or "").strip().lower()
        if not ca:
            skipped.append(r["slug"])
            continue

        spread_vals = {c: r[c] for c, _ in SPREAD_COLUMNS}
        try:
            existing = tgt.execute(
                "SELECT contract_address FROM collections WHERE contract_address = ?", (ca,)
            ).fetchone()
            if existing:
                sets = ", ".join(
                    ["slug = COALESCE(?, slug)", "name = COALESCE(?, name)",
                     "creator_fee_bps = COALESCE(?, creator_fee_bps)",
                     "opensea_fee_bps = COALESCE(?, opensea_fee_bps)",
                     "total_fee_bps = COALESCE(?, total_fee_bps)",
                     "floor_price_eth = COALESCE(?, floor_price_eth)",
                     "best_offer_eth = COALESCE(?, best_offer_eth)",
                     "floor_fetched_at = COALESCE(?, floor_fetched_at)",
                     "fetched_at = COALESCE(?, fetched_at)"]
                    + [f"{c} = ?" for c, _ in SPREAD_COLUMNS]
                )
                tgt.execute(
                    f"UPDATE collections SET {sets} WHERE contract_address = ?",
                    [r["slug"], r["name"], r["creator_fee_bps"], r["opensea_fee_bps"],
                     r["total_fee_bps"], r["floor_price_eth"], r["best_offer_eth"],
                     r["updated_at"] if r["floor_price_eth"] is not None else None,
                     r["updated_at"]]
                    + list(spread_vals.values()) + [ca],
                )
                updated += 1
            else:
                cols = (["contract_address", "slug", "name", "creator_fee_bps",
                         "opensea_fee_bps", "total_fee_bps", "floor_price_eth",
                         "best_offer_eth", "floor_fetched_at", "fetched_at"]
                        + [c for c, _ in SPREAD_COLUMNS])
                vals = ([ca, r["slug"], r["name"], r["creator_fee_bps"],
                         r["opensea_fee_bps"], r["total_fee_bps"], r["floor_price_eth"],
                         r["best_offer_eth"],
                         r["updated_at"] if r["floor_price_eth"] is not None else None,
                         r["updated_at"]]
                        + list(spread_vals.values()))
                tgt.execute(
                    f"INSERT INTO collections ({', '.join(cols)}) "
                    f"VALUES ({', '.join('?' * len(cols))})",
                    vals,
                )
                inserted += 1
        except sqlite3.IntegrityError as e:
            conflicts.append((r["slug"], ca, str(e)))

    print(f"  collections: {inserted} inserted, {updated} updated")
    if skipped:
        print(f"  WARNING: skipped (no contract_address): {skipped}")
    if conflicts:
        print("  WARNING: slug conflicts — same slug already on a different contract. "
              "Resolve manually:")
        for slug, ca, err in conflicts:
            print(f"    {slug} / {ca}: {err}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", default=DEFAULT_SOURCE, help="sister collection_trades.db")
    ap.add_argument("--target", default=DEFAULT_TARGET, help="nft_trades.db")
    args = ap.parse_args()

    source = os.path.abspath(args.source)
    target = os.path.abspath(args.target)
    if not os.path.exists(source):
        print(f"ERROR: source DB not found: {source}")
        return 1
    if not os.path.exists(target):
        print(f"ERROR: target DB not found: {target}")
        return 1

    print(f"Source: {source}")
    print(f"Target: {target}")

    bak = backup(target)
    print(f"Backup written: {bak}")

    tgt = sqlite3.connect(target, timeout=30)
    tgt.row_factory = sqlite3.Row
    tgt.execute("PRAGMA journal_mode=WAL")
    try:
        print("Adding spread columns to collections ...")
        for col, typ in SPREAD_COLUMNS:
            try:
                tgt.execute(f"ALTER TABLE collections ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass  # already exists

        print("Creating sales + collection_sync_state ...")
        tgt.executescript(NEW_TABLES)

        # Unique slug index: nice-to-have for integrity; skip (with a report) if
        # legacy rows already share a slug.
        dupes = tgt.execute(
            "SELECT slug, COUNT(*) n FROM collections "
            "WHERE slug IS NOT NULL AND slug != '' GROUP BY slug HAVING n > 1"
        ).fetchall()
        if dupes:
            print("  WARNING: duplicate slugs in collections, unique index NOT created:")
            for d in dupes:
                print(f"    {d['slug']} ({d['n']} rows)")
        else:
            tgt.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_collections_slug "
                "ON collections(slug) WHERE slug IS NOT NULL AND slug != ''"
            )

        tgt.execute("ATTACH DATABASE ? AS src", (source,))

        print("Merging collections ...")
        merge_collections(tgt)

        print("Copying sales ...")
        before = tgt.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        tgt.execute("""
            INSERT OR IGNORE INTO sales
                (collection_slug, tx_hash, nft_id, timestamp, price_eth,
                 payment_token, sale_type, seller, buyer)
            SELECT collection_slug, tx_hash, nft_id, timestamp, price_eth,
                   payment_token, sale_type, seller, buyer
            FROM src.sales
        """)
        after = tgt.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        src_count = tgt.execute("SELECT COUNT(*) FROM src.sales").fetchone()[0]
        print(f"  sales: {after - before} copied ({src_count} in source, {after} in target)")

        print("Copying sync state ...")
        tgt.execute("""
            INSERT OR IGNORE INTO collection_sync_state (collection_slug, oldest_ts_fetched, last_synced_at)
            SELECT collection_slug, oldest_ts_fetched, last_synced_at FROM src.sync_state
        """)

        tgt.commit()
        tgt.execute("DETACH DATABASE src")

        print("\nVerification:")
        for label, sql in [
            ("collections total", "SELECT COUNT(*) FROM collections"),
            ("collections with spread data",
             "SELECT COUNT(*) FROM collections WHERE avg_net_spread_pct IS NOT NULL"),
            ("sales rows", "SELECT COUNT(*) FROM sales"),
            ("collection_sync_state rows", "SELECT COUNT(*) FROM collection_sync_state"),
        ]:
            print(f"  {label}: {tgt.execute(sql).fetchone()[0]}")
        print("\nDone. Next: apply the sister-project code changes "
              "(see data/plan/merge-coldata-db.md).")
        return 0
    except Exception:
        tgt.rollback()
        print(f"\nFAILED — target rolled back. Backup available at: {bak}")
        raise
    finally:
        tgt.close()


if __name__ == "__main__":
    sys.exit(main())
