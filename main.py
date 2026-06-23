"""
NFT Trade Tracker - CLI

Usage:
    python main.py sync <wallet_address> [--gas] [--reset]
    python main.py report <wallet_address> [--top N]
    python main.py trades <wallet_address> [--collection SLUG]
"""

import argparse
import os
import sys
import time
import logging

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

import db
import fetch
import analytics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _ensure_collections(conn, events: list, seen_slugs: set):
    """Fetch and cache collection info for any new slugs seen in this page."""
    for ev in events:
        slug = ev.get("collection_slug", "")
        contract = ev.get("collection_address", "")
        if not slug or slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        existing = db.get_collection(conn, contract) if contract else None
        if existing:
            continue
        try:
            info = fetch.fetch_collection_info(slug)
            if info.get("contract_address"):
                contract = info["contract_address"]
            db.upsert_collection(
                conn,
                contract_address=contract,
                slug=slug,
                name=info.get("name", slug),
                creator_fee_bps=info.get("creator_fee_bps", 0),
                opensea_fee_bps=info.get("opensea_fee_bps", 250),
                fetched_at=int(time.time()),
            )
            log.info("Cached collection: %s (%s)", info.get("name", slug), slug)
        except Exception as e:
            log.warning("Could not fetch collection info for %s: %s", slug, e)
        time.sleep(0.25)


def _insert_page(conn, wallet: str, events: list, gas_map: dict) -> tuple[int, int]:
    """Insert one page of events. Returns (inserted, skipped)."""
    inserted = skipped = 0
    for ev in events:
        trade = {
            "wallet_address": wallet,
            "tx_hash": ev["tx_hash"],
            "block_timestamp": ev["block_timestamp"],
            "side": ev["side"],
            "eth_amount": ev["eth_amount"],
            "gas_eth": gas_map.get(ev["tx_hash"], 0.0),
            "buyer_address": ev["buyer_address"],
            "seller_address": ev["seller_address"],
            "collection_address": ev["collection_address"],
            "collection_slug": ev.get("collection_slug", ""),
            "nft_id": ev["nft_id"],
            "marketplace": ev.get("marketplace", "opensea"),
            "sell_type": ev.get("sell_type"),
        }
        if db.insert_trade(conn, trade):
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped


def cmd_sync(args):
    wallet = args.wallet.lower()
    db.init_db()

    with db.get_conn() as conn:
        state = db.get_sync_state(conn, wallet)

    # ── Determine start cursor ────────────────────────────────────────────────
    start_cursor = None
    incremental = False  # True = checking for new trades only (full sync already done)

    if args.reset or not state:
        log.info("Full sync — fetching all history from newest to oldest")
    elif not state["full_sync_complete"]:
        start_cursor = state["last_cursor"]
        if start_cursor:
            log.info("Resuming interrupted full sync from saved position "
                     "(last saved %s)", time.strftime("%Y-%m-%d %H:%M", time.localtime(state["last_synced_at"])))
        else:
            log.info("Starting full sync")
    else:
        # Full sync previously completed — fetch from the top to catch new trades
        incremental = True
        log.info("Incremental update — fetching new trades since last sync (%s)",
                 time.strftime("%Y-%m-%d %H:%M", time.localtime(state["last_synced_at"])))

    # ── Page-by-page fetch, process, save ────────────────────────────────────
    print(f"Fetching sale events for {wallet} ...")

    seen_slugs: set = set()
    total_inserted = total_skipped = 0
    cursor = start_cursor
    page_num = 0

    while True:
        events, next_cursor = fetch.fetch_sale_events(wallet, cursor)
        page_num += 1

        if not events:
            # No events on this page — we're done
            with db.get_conn() as conn:
                db.set_sync_state(conn, wallet, int(time.time()),
                                  last_cursor=None, full_sync_complete=True,
                                  total_inserted=total_inserted)
            break

        # ── Gas (optional) ────────────────────────────────────────────────────
        gas_map = {}
        if args.gas:
            gas_map = fetch.fetch_gas_for_trades(events)

        # ── Collections + trades ──────────────────────────────────────────────
        with db.get_conn() as conn:
            _ensure_collections(conn, events, seen_slugs)
            page_inserted, page_skipped = _insert_page(conn, wallet, events, gas_map)

            # Save cursor for the NEXT page so a restart continues from here
            full_done = next_cursor is None
            db.set_sync_state(conn, wallet, int(time.time()),
                              last_cursor=next_cursor,
                              full_sync_complete=full_done,
                              total_inserted=page_inserted)

        total_inserted += page_inserted
        total_skipped += page_skipped

        print(f"  Page {page_num}: +{page_inserted} new  ({total_inserted} total, {total_skipped} dupes)", end="\r")

        # ── Incremental stop condition ────────────────────────────────────────
        # If doing an incremental update and an entire page is all duplicates,
        # we've caught up — no point fetching older pages.
        if incremental and page_inserted == 0 and page_skipped > 0:
            log.info("\nAll events on page %d already stored — caught up.", page_num)
            with db.get_conn() as conn:
                db.set_sync_state(conn, wallet, int(time.time()),
                                  last_cursor=None, full_sync_complete=True,
                                  total_inserted=0)
            break

        if not next_cursor:
            break

        cursor = next_cursor
        time.sleep(0.3)

    print(f"\nSync complete: {total_inserted} new trades, {total_skipped} duplicates skipped.")


def cmd_wallet(args):
    db.init_db()
    with db.get_conn() as conn:
        if args.wallet_cmd == "set":
            if not args.name and not args.notes:
                print("Provide at least --name or --notes.")
                return
            db.upsert_wallet(conn, args.address, name=args.name, notes=args.notes)
            print(f"Saved: {args.address.lower()}"
                  + (f"  name='{args.name}'" if args.name else "")
                  + (f"  notes='{args.notes}'" if args.notes else ""))
        elif args.wallet_cmd == "list":
            from tabulate import tabulate
            import time as _time
            rows = db.list_wallets(conn)
            if not rows:
                print("No wallets saved yet.")
                return
            table = []
            for r in rows:
                r = dict(r)
                synced = _time.strftime("%Y-%m-%d", _time.localtime(r["last_synced_at"])) if r.get("last_synced_at") else "-"
                snap = _time.strftime("%Y-%m-%d", _time.localtime(r["computed_at"])) if r.get("computed_at") else "-"
                pnl = f"{r['realized_pnl_eth']:+.4f} ETH" if r.get("realized_pnl_eth") is not None else "-"
                trades_n = str(r["total_trades"]) if r.get("total_trades") is not None else "-"
                table.append([r["address"][:20] + "...", r.get("name") or "-", pnl, trades_n, snap, synced, r.get("notes") or ""])
            print(tabulate(table, headers=["Address", "Name", "PnL", "Trades", "Summary", "Synced", "Notes"], tablefmt="simple"))


def cmd_report(args):
    wallet = args.wallet.lower()
    db.init_db()

    with db.get_conn() as conn:
        trades = db.get_trades(conn, wallet)
        wallet_row = db.get_wallet(conn, wallet)
        latest_trade_ts = db.get_latest_trade_ts(conn, wallet)
        existing_summary = db.get_wallet_summary(conn, wallet)

    wallet_name = wallet_row["name"] if wallet_row and wallet_row["name"] else None

    if not trades:
        print(f"No trades found for {wallet}. Run: python main.py sync {wallet}")
        return

    result = analytics.compute_analytics(trades)

    # Always refresh the summary cache — stale if fees/code changed since last sync
    with db.get_conn() as conn:
        db.upsert_wallet_summary(conn, wallet, result["summary"], latest_trade_ts)

    # Print summary first
    print()
    print(analytics.format_summary(result, wallet_name=wallet_name))
    print()

    # Fetch current floor prices for open positions (cached, 7-day TTL)
    floor_prices = {}
    open_pos = result.get("open_positions", {})
    if open_pos:
        open_slugs = list(dict.fromkeys(
            b.get("collection_slug")
            for buys in open_pos.values()
            for b in buys
            if b.get("collection_slug")
        ))
        if open_slugs:
            now = int(time.time())
            stale_threshold = now - db.FLOOR_CACHE_TTL_SECS
            with db.get_conn() as conn:
                cached = db.get_cached_floors(conn, open_slugs)
                stale_slugs = [
                    s for s in open_slugs
                    if s not in cached
                    or cached[s]["floor_fetched_at"] is None
                    or cached[s]["floor_fetched_at"] < stale_threshold
                ]
                if stale_slugs:
                    print(f"Fetching floor prices for {len(stale_slugs)} collection(s) ...")
                for slug in open_slugs:
                    row = cached.get(slug)
                    if row and row["floor_fetched_at"] and row["floor_fetched_at"] >= stale_threshold:
                        if row["floor_price_eth"] is not None:
                            floor_prices[slug] = row["floor_price_eth"]
                        continue
                    try:
                        fp = fetch.fetch_floor_price(slug)
                        bo = fetch.fetch_best_offer(slug)
                        time.sleep(0.25)
                        db.upsert_collection_floor(conn, slug, fp, bo, now)
                        if fp is not None:
                            floor_prices[slug] = fp
                    except Exception:
                        pass

    print("=== PnL BY COLLECTION ===")
    print(analytics.format_collections(result, top_n=args.top))
    print()
    print("=== OPEN POSITIONS ===")
    print(analytics.format_open_positions(result, floor_prices=floor_prices or None))
    print()


def cmd_trades(args):
    wallet = args.wallet.lower()
    db.init_db()

    with db.get_conn() as conn:
        if args.collection:
            rows = conn.execute("""
                SELECT t.*, c.name AS collection_name
                FROM trades t
                LEFT JOIN collections c ON t.collection_address = c.contract_address
                WHERE t.wallet_address = ?
                  AND (t.collection_slug = ? OR t.collection_address = ?)
                ORDER BY t.block_timestamp ASC
            """, (wallet, args.collection, args.collection.lower())).fetchall()
        else:
            rows = db.get_trades(conn, wallet)

    if not rows:
        print("No trades found.")
        return

    from tabulate import tabulate
    from datetime import datetime, timezone

    table = []
    for r in rows:
        row = dict(r)
        col_name = (row.get("collection_name") or row.get("collection_slug") or row["collection_address"][:10])[:22]
        ts = datetime.fromtimestamp(row["block_timestamp"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        table.append([
            ts,
            row["side"].upper(),
            f"{row['eth_amount']:.4f}",
            f"{row.get('gas_eth', 0):.5f}",
            col_name,
            row["nft_id"],
            row["tx_hash"][:12] + "...",
        ])

    headers = ["Timestamp (UTC)", "Side", "ETH", "Gas ETH", "Collection", "NFT ID", "Tx"]
    print(tabulate(table, headers=headers, tablefmt="simple"))
    print(f"\nTotal: {len(rows)} trades")


def main():
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="NFT trade tracker — sync and analyze wallet trading history",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # sync
    p_sync = sub.add_parser("sync", help="Fetch and store trade data for a wallet")
    p_sync.add_argument("wallet", help="Ethereum wallet address (0x...)")
    p_sync.add_argument("--gas", action="store_true",
                        help="Fetch gas costs from Etherscan (slower, uses API quota)")
    p_sync.add_argument("--reset", action="store_true",
                        help="Ignore saved cursor and re-fetch all history")

    # report
    p_report = sub.add_parser("report", help="Print PnL summary and per-collection stats")
    p_report.add_argument("wallet", help="Ethereum wallet address (0x...)")
    p_report.add_argument("--top", type=int, default=20, help="Show top N collections (default: 20)")

    # trades
    p_trades = sub.add_parser("trades", help="List individual trades")
    p_trades.add_argument("wallet", help="Ethereum wallet address (0x...)")
    p_trades.add_argument("--collection", default=None,
                          help="Filter by collection slug or contract address")

    # wallet
    p_wallet = sub.add_parser("wallet", help="Manage wallet names and notes")
    wallet_sub = p_wallet.add_subparsers(dest="wallet_cmd", required=True)

    p_wset = wallet_sub.add_parser("set", help="Set name/notes for a wallet address")
    p_wset.add_argument("address", help="Ethereum wallet address (0x...)")
    p_wset.add_argument("--name", default=None, help="Human-readable label")
    p_wset.add_argument("--notes", default=None, help="Free-text notes")

    wallet_sub.add_parser("list", help="List all saved wallets")

    args = parser.parse_args()

    # Validate API keys (not needed for wallet subcommand)
    if args.command != "wallet":
        if not os.environ.get("OPENSEA_API_KEY"):
            print("ERROR: OPENSEA_API_KEY not set. Copy .env.example to .env and fill in your keys.")
            sys.exit(1)
        if args.command == "sync" and args.gas and not os.environ.get("ETHERSCAN_API_KEY"):
            print("ERROR: ETHERSCAN_API_KEY not set (required for --gas).")
            sys.exit(1)

    dispatch = {"sync": cmd_sync, "report": cmd_report, "trades": cmd_trades, "wallet": cmd_wallet}
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
