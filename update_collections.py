#!/usr/bin/env python3
"""Sync market-wide collection sale data (the Dump/Listing Buy Ratio tab's source).

Ported from the sister project's update_all.py / collection_ev.py, adapted to the
merged nft_trades.db schema (collections keyed by contract_address, sync state in
collection_sync_state).

For each tracked collection (rows in collection_sync_state):
  1. Re-fetch metadata (fees, contract address) and current floor/best offer.
  2. Forward-sync new sale events since last sync (sale_type: WETH = bid, else listing).
  3. Backfill any historical sales not yet stored.
  4. Recompute trimmed-mean daily spread + volume stats into collections.

Usage:
    python update_collections.py                  # sync all tracked collections
    python update_collections.py --slug SLUG      # only this slug (repeatable);
                                                  # an untracked slug gets a full
                                                  # history import and becomes tracked
    python update_collections.py --prices-only    # refresh prices/metadata only

Intended to run from cron on the server, e.g. daily:
    0 5 * * * cd /opt/nft-analysis && flock -n /tmp/nft-colsync.lock \\
        venv/bin/python update_collections.py >> /var/log/nft-analysis/colsync.log 2>&1
"""

import argparse
import os
import time

import requests
from dotenv import load_dotenv

import db

load_dotenv()

OPENSEA_BASE = "https://api.opensea.io/api/v2"
OPENSEA_FEE_RECIPIENT = "0x0000a26b00c1f0df003000390027140000faa719"
ETH_TOKENS = {
    "0x0000000000000000000000000000000000000000",  # native ETH
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
}


# ── OpenSea fetch layer ────────────────────────────────────────────────────────

def _headers() -> dict:
    return {"accept": "application/json", "x-api-key": os.environ.get("OPENSEA_API_KEY", "")}


def _get(url: str, params: dict, retries: int = 8) -> dict:
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=15)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else min(2 ** attempt, 60)
                except ValueError:
                    wait = min(2 ** attempt, 60)
                print(f"\n  rate limited, waiting {wait:.0f}s (attempt {attempt + 1}/{retries})...", flush=True)
                time.sleep(wait)
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait = min(2 ** attempt, 60)
                print(f"\n  server error {resp.status_code}, retrying in {wait:.0f}s "
                      f"(attempt {attempt + 1}/{retries})...", flush=True)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except (requests.Timeout, requests.ConnectionError) as e:
            wait = min(2 ** attempt, 60)
            print(f"\n  {type(e).__name__}, retrying in {wait:.0f}s (attempt {attempt + 1}/{retries})...", flush=True)
            time.sleep(wait)
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(min(2 ** attempt, 60))

    raise RuntimeError(f"Failed to GET {url} after {retries} attempts")


def resolve_collection(ca_or_slug: str) -> dict:
    """Resolve contract address or slug to collection metadata incl. fee split."""
    if ca_or_slug.startswith("0x"):
        data = _get(f"{OPENSEA_BASE}/chain/ethereum/contract/{ca_or_slug}", {})
        slug = data.get("collection", "")
        if not slug:
            raise RuntimeError(f"Could not find collection for contract {ca_or_slug}")
    else:
        slug = ca_or_slug

    data = _get(f"{OPENSEA_BASE}/collections/{slug}", {})
    if not data:
        raise RuntimeError(f"Collection '{slug}' not found on OpenSea")

    creator_fee_bps = 0
    opensea_fee_bps = 0
    for fee in data.get("fees", []):
        recipient = (fee.get("recipient") or "").lower()
        bps = int(round(float(fee.get("fee", 0)) * 100))
        if recipient == OPENSEA_FEE_RECIPIENT:
            opensea_fee_bps += bps
        else:
            creator_fee_bps += bps

    contract_address = ""
    for c in data.get("contracts", []):
        if c.get("chain", "").lower() == "ethereum":
            contract_address = c.get("address", "").lower()
            break

    return {
        "slug": slug,
        "name": data.get("name", slug),
        "contract_address": contract_address,
        "creator_fee_bps": creator_fee_bps,
        "opensea_fee_bps": opensea_fee_bps,
        "total_fee_bps": creator_fee_bps + opensea_fee_bps,
    }


def fetch_market_prices(slug: str) -> dict:
    """Fetch current floor and best offer, return {floor, best_offer}."""
    floor = None
    best_offer = None

    try:
        stats = _get(f"{OPENSEA_BASE}/collections/{slug}/stats", {})
        raw = (stats.get("total") or {}).get("floor_price")
        if raw is not None:
            floor = float(raw)
    except Exception:
        pass

    try:
        offers_data = _get(f"{OPENSEA_BASE}/offers/collection/{slug}", {})
        offers = offers_data.get("offers") or []
        if offers:
            price = offers[0]["price"]
            raw_val = price.get("value") or (price.get("current") or {}).get("value")
            if raw_val is not None:
                best_offer = int(raw_val) / 1e18
    except Exception:
        pass

    return {"floor": floor, "best_offer": best_offer}


def fetch_collection_events(slug, since_ts, occurred_before=None,
                            on_checkpoint=None, checkpoint_every=1000) -> list:
    """Paginate all ETH sale events for a collection since since_ts.

    If occurred_before is set, pagination starts from that timestamp going
    backwards. on_checkpoint (if given) is called with each batch of
    checkpoint_every accumulated events so callers can persist mid-run.
    """
    events = []
    last_checkpoint = 0
    cursor = None
    page = 0
    done = False

    while not done:
        params = {"event_type": "sale", "chain": "ethereum", "limit": 50}
        if cursor:
            params["next"] = cursor
        elif occurred_before:
            params["before"] = occurred_before

        data = _get(f"{OPENSEA_BASE}/events/collection/{slug}", params)
        raw = data.get("asset_events", [])
        next_cursor = data.get("next")
        page += 1

        if page == 1 or page % 10 == 0:
            print(f"  page {page}  ({len(events)} events so far)...", end="\r", flush=True)

        for ev in raw:
            ts = ev.get("closing_date") or 0
            if ts < since_ts:
                done = True
                break

            payment = ev.get("payment") or {}
            token_addr = (payment.get("token_address") or "").lower()
            symbol = (payment.get("symbol") or "").upper()
            if token_addr not in ETH_TOKENS and symbol not in ("ETH", "WETH"):
                continue

            try:
                price_eth = int(payment.get("quantity", "0")) / 1e18
            except (ValueError, TypeError):
                continue
            if price_eth <= 0:
                continue

            nft = ev.get("nft") or {}
            events.append({
                "tx_hash": (ev.get("transaction") or "").lower(),
                "nft_id": str(nft.get("identifier", "")),
                "timestamp": ts,
                "price_eth": price_eth,
                "payment_token": symbol,
                "sale_type": "bid" if symbol == "WETH" else "listing",
                "seller": (ev.get("seller") or "").lower(),
                "buyer": (ev.get("buyer") or "").lower(),
            })

        if on_checkpoint and len(events) - last_checkpoint >= checkpoint_every:
            on_checkpoint(events[last_checkpoint:])
            last_checkpoint = len(events)

        if not next_cursor or not raw:
            done = True
        elif not done:
            cursor = next_cursor
            time.sleep(0.25)

    print(f"  done - {len(events):,} ETH sale events collected across {page} pages.", flush=True)
    return events


# ── Spread / volume stats ──────────────────────────────────────────────────────

def _trimmed_mean(values: list, trim: float = 0.10) -> float:
    """Mean after discarding the bottom and top `trim` fraction of values."""
    n = len(values)
    k = int(n * trim)
    if k == 0:
        return sum(values) / n
    vals = sorted(values)[k:-k]
    return sum(vals) / len(vals)


def compute_daily_avg_spread(sales: list, total_fee_bps: int) -> dict:
    """Per day with both listing and bid activity:
    gross = avg_listing - avg_bid; net = gross - listing fees.
    Final averages are 10% trimmed means across days.
    """
    daily = {}
    for s in sales:
        day = s["timestamp"] // 86_400
        buckets = daily.setdefault(day, {"listing": [], "bid": []})
        if s["sale_type"] in buckets:
            buckets[s["sale_type"]].append(s["price_eth"])

    fee_rate = total_fee_bps / 10_000
    gross_eths, net_eths, gross_pcts, net_pcts = [], [], [], []
    for buckets in daily.values():
        if not buckets["listing"] or not buckets["bid"]:
            continue
        avg_listing = sum(buckets["listing"]) / len(buckets["listing"])
        avg_bid = sum(buckets["bid"]) / len(buckets["bid"])
        mid = (avg_listing + avg_bid) / 2
        gross = avg_listing - avg_bid
        net = gross - avg_listing * fee_rate
        gross_eths.append(gross)
        net_eths.append(net)
        if mid:
            gross_pcts.append(gross / mid * 100)
            net_pcts.append(net / mid * 100)

    if not gross_eths:
        return {}

    return {
        "avg_gross_spread_eth": _trimmed_mean(gross_eths),
        "avg_net_spread_eth": _trimmed_mean(net_eths),
        "avg_gross_spread_pct": _trimmed_mean(gross_pcts) if gross_pcts else None,
        "avg_net_spread_pct": _trimmed_mean(net_pcts) if net_pcts else None,
        "pair_count": len(gross_eths),
    }


def compute_daily_volume(sales: list) -> dict:
    """Trimmed-mean daily sale count, all-time and last 30 days.
    Fills zeros for inactive days within each window."""
    if not sales:
        return {"avg_daily_sales_alltime": None, "avg_daily_sales_30d": None, "total_trades": 0}

    now = int(time.time())
    daily = {}
    for s in sales:
        d = s["timestamp"] // 86_400
        daily[d] = daily.get(d, 0) + 1

    all_days = sorted(daily)
    alltime_counts = [daily.get(d, 0) for d in range(all_days[0], all_days[-1] + 1)]

    day_cutoff = (now - 30 * 86_400) // 86_400
    day_now = now // 86_400
    counts_30d = [daily.get(d, 0) for d in range(day_cutoff, day_now + 1)]

    return {
        "avg_daily_sales_alltime": _trimmed_mean(alltime_counts),
        "avg_daily_sales_30d": _trimmed_mean(counts_30d) if counts_30d else None,
        "total_trades": len(sales),
    }


# ── Per-collection update ──────────────────────────────────────────────────────

def update_collection(conn, slug: str, prices_only: bool) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {slug}")
    print(f"{'=' * 60}")

    print("  Refreshing metadata & prices...")
    try:
        collection = resolve_collection(slug)
    except Exception as e:
        print(f"  ERROR resolving collection: {e}")
        return

    try:
        prices = fetch_market_prices(slug)
    except Exception as e:
        print(f"  WARNING: could not fetch prices: {e}")
        prices = {"floor": None, "best_offer": None}

    floor_disp = f"{prices['floor']:.4f}" if prices["floor"] is not None else "N/A"
    offer_disp = f"{prices['best_offer']:.4f}" if prices["best_offer"] is not None else "N/A"
    print(f"  floor: {floor_disp} ETH  |  best offer: {offer_disp} ETH  |  "
          f"fees: {collection['creator_fee_bps'] / 100:.2f}% + {collection['opensea_fee_bps'] / 100:.2f}% OS")

    if not db.upsert_collection_market(conn, collection, prices):
        print(f"  WARNING: no contract address for {slug} — collection row not updated")

    if prices_only:
        print("  (prices-only mode — skipping sales fetch)")
        return

    def make_checkpoint(label: str):
        def checkpoint(batch: list) -> None:
            saved = db.insert_sales(conn, slug, batch)
            oldest_in_batch = min(e["timestamp"] for e in batch)
            db.update_coldata_sync_state(conn, slug, oldest_in_batch)
            print(f"\n  [{label} checkpoint] saved {saved:,} new events "
                  f"(oldest: {time.strftime('%Y-%m-%d', time.localtime(oldest_in_batch))})...",
                  flush=True)
        return checkpoint

    sync = db.get_coldata_sync_state(conn, slug)

    if sync is None:
        print("  No sync state — fetching full history...")
        new_events = fetch_collection_events(slug, since_ts=0, on_checkpoint=make_checkpoint("import"))
        if new_events:
            inserted = db.insert_sales(conn, slug, new_events)
            db.update_coldata_sync_state(conn, slug, min(e["timestamp"] for e in new_events))
            print(f"  stored {inserted:,} new events")
        else:
            print("  no events found")
            return
    else:
        # Forward sync: new events since last sync
        last_sync = sync["last_synced_at"]
        print(f"  Forward sync from {time.strftime('%Y-%m-%d', time.localtime(last_sync))}...")
        new_events = fetch_collection_events(slug, since_ts=last_sync, on_checkpoint=make_checkpoint("forward"))
        if new_events:
            inserted = db.insert_sales(conn, slug, new_events)
            db.update_coldata_sync_state(conn, slug, sync["oldest_ts_fetched"])
            print(f"  stored {inserted:,} new events")
        else:
            print("  no new events since last sync")

        # Backfill: historical events older than oldest stored
        oldest_ts = db.get_coldata_sync_state(conn, slug)["oldest_ts_fetched"]
        print(f"  Backfilling history before {time.strftime('%Y-%m-%d', time.localtime(oldest_ts))}...")
        old_events = fetch_collection_events(
            slug, since_ts=0, occurred_before=oldest_ts,
            on_checkpoint=make_checkpoint("backfill"),
        )
        if old_events:
            inserted = db.insert_sales(conn, slug, old_events)
            db.update_coldata_sync_state(conn, slug, min(e["timestamp"] for e in old_events))
            print(f"  backfilled {inserted:,} historical events")
        else:
            print("  no additional historical events found")

    # Recompute spread + volume stats over all stored sales
    if not collection["contract_address"]:
        return
    all_sales = db.get_sales(conn, slug)
    spread = compute_daily_avg_spread(all_sales, collection["total_fee_bps"])
    volume = compute_daily_volume(all_sales)
    if spread:
        db.update_collection_spread(conn, collection["contract_address"], {**spread, **volume})
        print(f"  spread: {spread['avg_gross_spread_eth']:+.4f} ETH gross / "
              f"{spread['avg_net_spread_eth']:+.4f} ETH net "
              f"({spread['pair_count']:,} days)  |  "
              f"vol: {volume['avg_daily_sales_alltime']:.1f}/d alltime  "
              f"{volume['avg_daily_sales_30d']:.1f}/d 30d")


def main():
    parser = argparse.ArgumentParser(description="Sync market-wide collection sale data")
    parser.add_argument("--slug", action="append", dest="slugs", metavar="SLUG",
                        help="Only update this slug (repeatable). A new slug gets a "
                             "full history import and becomes tracked. Default: all tracked.")
    parser.add_argument("--prices-only", action="store_true",
                        help="Refresh prices/metadata only; skip fetching new sales.")
    args = parser.parse_args()

    db.init_db()
    conn = db.get_conn()

    slugs = args.slugs or db.list_coldata_slugs(conn)
    if not slugs:
        print("No tracked collections in collection_sync_state. "
              "Track one with: python update_collections.py --slug <slug>")
        return

    print(f"Updating {len(slugs)} collection(s)...")
    start = time.time()

    for i, slug in enumerate(slugs, 1):
        print(f"\n[{i}/{len(slugs)}] {slug}")
        update_collection(conn, slug, prices_only=args.prices_only)

    print(f"\nDone. Updated {len(slugs)} collection(s) in {time.time() - start:.0f}s.")
    conn.close()


if __name__ == "__main__":
    main()
