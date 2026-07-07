"""Flask web frontend for NFT Player Analysis."""

import json
import logging
import os
import subprocess
import sys
import time as _time
from datetime import datetime, timezone as _tz

log = logging.getLogger(__name__)

import requests as _req
from flask import Flask, Response, jsonify, render_template, request, stream_with_context

import analytics
import db
import fetch

app = Flask(__name__)


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── API: wallets ───────────────────────────────────────────────────────────────

def _wallet_stats_from_matched(matched):
    """Recompute wallet summary stats from a filtered list of matched round-trips."""
    if not matched:
        return {"realized_pnl_eth": 0.0, "total_trades": 0, "win_rate": None,
                "avg_holding_secs": None, "total_buy_eth": 0.0}
    pnl = sum(m["pnl_eth"] for m in matched)
    wins = sum(1 for m in matched if m["pnl_eth"] > 0)
    holds = [m["holding_secs"] for m in matched if m["holding_secs"] >= 0]
    return {
        "realized_pnl_eth": pnl,
        "total_trades": len(matched),
        "win_rate": wins / len(matched),
        "avg_holding_secs": sum(holds) / len(holds) if holds else None,
        "total_buy_eth": sum(m["buy_eth"] for m in matched),
    }


@app.route("/api/wallets")
def api_wallets():
    sell_from  = request.args.get("sell_from",  type=int)  # Unix timestamp, inclusive
    sell_until = request.args.get("sell_until", type=int)  # Unix timestamp, inclusive
    db.init_db()
    with db.get_conn() as conn:
        rows = conn.execute("""
            SELECT w.address, w.name, w.notes,
                   s.last_synced_at, s.full_sync_complete,
                   ws.realized_pnl_eth, ws.total_trades, ws.win_rate,
                   ws.collections_traded, ws.computed_at, ws.open_positions,
                   ws.avg_holding_secs, ws.total_buy_eth
            FROM wallets w
            LEFT JOIN sync_state s        ON w.address = s.wallet_address
            LEFT JOIN wallet_summaries ws ON w.address = ws.wallet_address
            ORDER BY ws.realized_pnl_eth DESC, w.name
        """).fetchall()

        result = []
        for r in rows:
            d = dict(r)
            if sell_from is not None or sell_until is not None:
                ar = get_cached_analytics(conn, d["address"])
                matched = [
                    m for m in ar.get("matched_trades", [])
                    if (sell_from  is None or m["sell_ts"] >= sell_from)
                    and (sell_until is None or m["sell_ts"] <= sell_until)
                ]
                d.update(_wallet_stats_from_matched(matched))
            result.append(d)
    return jsonify(result)


# ── API: report ────────────────────────────────────────────────────────────────

@app.route("/api/report/<address>")
def api_report(address):
    address = address.lower()
    since = request.args.get("since", type=int)
    db.init_db()
    with db.get_conn() as conn:
        # Raw trades are still needed for the player card and slug mapping
        trades = db.get_trades(conn, address, since=since)
        wallet_row = db.get_wallet(conn, address)
        sync_state = db.get_sync_state(conn, address)
        latest_trade_ts = db.get_latest_trade_ts(conn, address)

        if not trades:
            msg = "No trades in this time range." if since else "No trades found. Run a sync first."
            return jsonify({"error": msg}), 404

        if since:
            result = analytics.compute_analytics(trades)
        else:
            result = get_cached_analytics(conn, address)
            # Keep the all-time summary cache fresh even on cache hits
            db.upsert_wallet_summary(conn, address, result["summary"], latest_trade_ts)

    # Player card — uses unstripped per_collection (still has holding_times)
    addr_to_slug = {}
    for t in trades:
        if t["collection_slug"] and t["collection_address"]:
            addr_to_slug[t["collection_address"]] = t["collection_slug"]
    _slugs = list(set(addr_to_slug.values()))
    floor_data = {}
    if _slugs:
        with db.get_conn() as conn:
            floor_data = db.get_cached_floors(conn, _slugs)
    player_card = analytics.compute_player_card(
        trades, result["per_collection"], result["summary"], floor_data
    )

    # Strip non-serializable holding_times list
    per_col = {}
    for addr, s in result["per_collection"].items():
        s2 = dict(s)
        s2.pop("holding_times", None)
        per_col[addr] = s2

    return jsonify({
        "wallet": {
            "address": address,
            "name": wallet_row["name"] if wallet_row else None,
            "notes": wallet_row["notes"] if wallet_row else None,
        },
        "summary": result["summary"],
        "per_collection": per_col,
        "open_positions": result.get("open_positions", {}),
        "sync_state": dict(sync_state) if sync_state else None,
        "filter_since": since,
        "player_card": player_card,
    })


# ── API: trades ────────────────────────────────────────────────────────────────

@app.route("/api/trades/<address>")
def api_trades(address):
    address = address.lower()
    db.init_db()
    collection = request.args.get("collection", "").strip()
    since = request.args.get("since", type=int)
    since_clause = "AND t.block_timestamp >= ?" if since else ""
    with db.get_conn() as conn:
        if collection:
            params = [address, collection, collection.lower()]
            if since:
                params.append(since)
            rows = conn.execute(f"""
                SELECT t.*, c.name AS collection_name
                FROM trades t
                LEFT JOIN collections c ON t.collection_address = c.contract_address
                WHERE t.wallet_address = ?
                  AND (t.collection_slug = ? OR t.collection_address = ?)
                  {since_clause}
                ORDER BY t.block_timestamp DESC
            """, params).fetchall()
        else:
            params = [address]
            if since:
                params.append(since)
            rows = conn.execute(f"""
                SELECT t.*, c.name AS collection_name
                FROM trades t
                LEFT JOIN collections c ON t.collection_address = c.contract_address
                WHERE t.wallet_address = ?
                  {since_clause}
                ORDER BY t.block_timestamp DESC
            """, params).fetchall()
    return jsonify([dict(r) for r in rows])


# ── API: PnL buckets ───────────────────────────────────────────────────────────

@app.route("/api/pnl_buckets/<address>")
def api_pnl_buckets(address):
    address = address.lower()
    since = request.args.get("since", type=int, default=0)
    db.init_db()
    with db.get_conn() as conn:
        if since:
            trades = db.get_trades(conn, address, since=since)
            result = analytics.compute_analytics(trades) if trades else {}
        else:
            result = get_cached_analytics(conn, address)
    if not result:
        return jsonify({"buckets": [], "bucket_type": "daily", "total_pnl_eth": 0})

    matched = result.get("matched_trades", [])

    buckets_map = {}
    for m in matched:
        _add_daily_bucket(buckets_map, m)

    buckets = sorted(buckets_map.values(), key=lambda b: b["key"])
    total_pnl = sum(b["pnl_eth"] for b in buckets)
    return jsonify({"buckets": buckets, "bucket_type": "daily", "total_pnl_eth": total_pnl})


def _add_daily_bucket(buckets_map, m):
    dt = datetime.fromtimestamp(m["sell_ts"], tz=_tz.utc)
    key = dt.strftime("%Y-%m-%d")
    if key not in buckets_map:
        day_ts = int(datetime(dt.year, dt.month, dt.day, tzinfo=_tz.utc).timestamp())
        buckets_map[key] = {"key": key, "ts": day_ts, "pnl_eth": 0.0, "trade_count": 0}
    buckets_map[key]["pnl_eth"] += m["pnl_eth"]
    buckets_map[key]["trade_count"] += 1


# ── API: sync (streaming) ──────────────────────────────────────────────────────

@app.route("/api/sync", methods=["POST"])
def api_sync():
    data = request.get_json() or {}
    address = (data.get("address") or "").strip()
    name = (data.get("name") or "").strip()
    gas = bool(data.get("gas"))
    reset = bool(data.get("reset"))

    import re
    if not address or not re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
        return jsonify({"error": "Invalid Ethereum address (must be 0x + 40 hex chars)"}), 400

    # Ensure wallet record exists before sync starts
    db.init_db()
    with db.get_conn() as conn:
        db.upsert_wallet(conn, address, name=name if name else None)

    def generate():
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        cmd = [sys.executable, "-u", "main.py", "sync", address]
        if gas:
            cmd.append("--gas")
        if reset:
            cmd.append("--reset")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=os.path.dirname(os.path.abspath(__file__)),
                env=env,
            )
            for line in proc.stdout:
                line = line.rstrip("\r\n")
                if line:
                    yield f"data: {json.dumps({'type': 'log', 'message': line})}\n\n"
            proc.wait()
            if proc.returncode == 0:
                yield f"data: {json.dumps({'type': 'done', 'address': address.lower()})}\n\n"
            else:
                yield f"data: {json.dumps({'type': 'error', 'message': f'Sync failed (exit {proc.returncode})'})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ── API: update wallet ────────────────────────────────────────────────────────

@app.route("/api/wallet/<address>", methods=["PATCH"])
def api_wallet_update(address):
    address = address.lower()
    data = request.get_json() or {}
    name = data.get("name")
    notes = data.get("notes")
    db.init_db()
    with db.get_conn() as conn:
        db.upsert_wallet(conn, address, name=name, notes=notes)
    return jsonify({"ok": True})


# ── API: floor prices + unrealized PnL ────────────────────────────────────────

@app.route("/api/floor/<address>")
def api_floor(address):
    import time as _time
    address = address.lower()
    db.init_db()
    with db.get_conn() as conn:
        trades = db.get_trades(conn, address)

        if not trades:
            return jsonify({"error": "No trades"}), 404

        result = analytics.compute_analytics(trades)
        open_positions = result.get("open_positions", {})

        if not open_positions:
            return jsonify({"upnl_eth": None, "floor_value_eth": None,
                            "cost_basis_eth": 0, "floor_prices": {}})

        # Filter out positions where the NFT is no longer held (transferred/sent away).
        # On exception (rate limit, network error) we fall through and show unfiltered positions.
        try:
            held_nfts = fetch.fetch_wallet_nfts(address)
            open_positions = {
                k: buys for k, buys in open_positions.items()
                if (k.split(":", 1)[0], k.split(":", 1)[1]) in held_nfts
            }
        except Exception as e:
            log.warning("Could not fetch wallet holdings, uPnL may include transferred NFTs: %s", e)

        if not open_positions:
            return jsonify({"upnl_eth": None, "floor_value_eth": None,
                            "cost_basis_eth": 0, "floor_prices": {},
                            "transferred_away": True})

        # Collect unique slugs across all open buys
        slug_to_fee = {}
        for buys in open_positions.values():
            for b in buys:
                slug = b.get("collection_slug")
                if slug:
                    slug_to_fee[slug] = b.get("total_fee_bps") or 0

        now = int(_time.time())
        stale_threshold = now - db.FLOOR_CACHE_TTL_SECS
        cached = db.get_cached_floors(conn, list(slug_to_fee.keys()))

        floor_prices = {}
        stale_slugs = [
            slug for slug in slug_to_fee
            if slug not in cached
            or cached[slug]["floor_fetched_at"] is None
            or cached[slug]["floor_fetched_at"] < stale_threshold
        ]

        # Populate floor_prices and bid_prices from cache for fresh slugs
        stale_set = set(stale_slugs)
        floor_prices = {}
        bid_prices = {}
        for slug, row in cached.items():
            if slug not in stale_set:
                if row["floor_price_eth"] is not None:
                    floor_prices[slug] = row["floor_price_eth"]
                if row["best_offer_eth"] is not None:
                    bid_prices[slug] = row["best_offer_eth"]

        # Fetch only stale/missing slugs from OpenSea
        for slug in stale_slugs:
            try:
                fp = fetch.fetch_floor_price(slug)
                bo = fetch.fetch_best_offer(slug)
                _time.sleep(0.25)
                db.upsert_collection_floor(conn, slug, fp, bo, now)
                if fp is not None:
                    floor_prices[slug] = fp
                if bo is not None:
                    bid_prices[slug] = bo
            except Exception:
                stale_row = cached.get(slug)
                if stale_row:
                    if stale_row["floor_price_eth"] is not None:
                        floor_prices[slug] = stale_row["floor_price_eth"]
                    if stale_row["best_offer_eth"] is not None:
                        bid_prices[slug] = stale_row["best_offer_eth"]

        # Compute totals
        total_cost = 0.0
        total_floor_net = 0.0
        total_bid_net = 0.0
        positions_with_floor = 0
        positions_with_bid = 0

        for buys in open_positions.values():
            for b in buys:
                cost = b["eth_amount"] + (b.get("gas_eth") or 0)
                total_cost += cost
                slug = b.get("collection_slug")
                fee_bps = b.get("total_fee_bps") or 0
                floor = floor_prices.get(slug) if slug else None
                if floor is not None:
                    total_floor_net += floor * (1 - fee_bps / 10000)
                    positions_with_floor += 1
                bid = bid_prices.get(slug) if slug else None
                if bid is not None:
                    total_bid_net += bid * (1 - fee_bps / 10000)
                    positions_with_bid += 1

        upnl = (total_floor_net - total_cost) if positions_with_floor else None
        upnl_bid = (total_bid_net - total_cost) if positions_with_bid else None

        return jsonify({
            "upnl_eth": upnl,
            "upnl_bid_eth": upnl_bid,
            "floor_value_eth": total_floor_net if positions_with_floor else None,
            "bid_value_eth": total_bid_net if positions_with_bid else None,
            "cost_basis_eth": total_cost,
            "floor_prices": floor_prices,
            "positions_with_floor": positions_with_floor,
            "positions_with_bid": positions_with_bid,
            "total_open": sum(len(v) for v in open_positions.values()),
        })


# ── API: meta analysis (cross-wallet collection stats, per time window) ────────

# Windows in days; 0 = all time. Windowed buckets count a round trip when its
# SELL falls inside the window (the buy may be older), so FIFO matching always
# runs over full history. All windows are computed in one pass and returned
# together so the frontend can switch windows without refetching.
META_WINDOWS = [1, 7, 30, 90, 0]

# Recomputing FIFO analytics for every wallet takes seconds, so cache the
# payload. Invalidated when the trades table changes (fingerprint) or after
# 5 minutes (window cutoffs drift with the clock).
_meta_cache = {"key": None, "expires": 0.0, "payload": None}

# Per-wallet analytics cache. FIFO analytics over full history is deterministic,
# so cache each wallet's compute_analytics() result keyed by a cheap fingerprint
# of that wallet's trade rows plus the collections table (get_trades JOINs
# collections for name/total_fee_bps, and fee bps feeds PnL math).
# TOTAL(gas_eth) catches --gas backfill UPDATEs; sell_type backfill UPDATEs are
# NOT caught (cosmetic only — /api/sell_graph bypasses this cache and stays fresh).
# Memory: ~23 wallets x full matched-trade/open-position dicts is tens of MB —
# fine for a local single-user tool.
_wallet_cache = {}  # wallet -> {"fp": (...), "result": analytics_dict}


def get_cached_analytics(conn, wallet):
    """Full-history analytics for one wallet, cached. Only call with a wallet's
    complete unfiltered history — filtered paths must fetch+compute themselves."""
    wallet = wallet.lower()
    trades_fp = tuple(conn.execute(
        "SELECT COUNT(*), COALESCE(MAX(id), 0), TOTAL(gas_eth) FROM trades WHERE wallet_address = ?",
        (wallet,)).fetchone())
    # upsert_collection_floor doesn't bump fetched_at, but floor data isn't in
    # get_trades output; COUNT + MAX(fetched_at) + TOTAL(total_fee_bps) covers
    # the columns that do flow into analytics (name changes ride on fetched_at).
    cols_fp = tuple(conn.execute(
        "SELECT COUNT(*), COALESCE(MAX(fetched_at), 0), TOTAL(total_fee_bps) FROM collections"
    ).fetchone())
    fp = trades_fp + cols_fp
    hit = _wallet_cache.get(wallet)
    if hit and hit["fp"] == fp:
        return hit["result"]
    # A sync subprocess may insert rows between the fingerprint query and this
    # fetch; the cached result then holds more trades than fp claims, and the
    # next request's fingerprint mismatch forces a recompute — self-healing.
    trades = db.get_trades(conn, wallet)
    result = analytics.compute_analytics(trades) if trades else {}
    # Flask runs threaded: dict assignment is atomic; worst case two threads
    # compute the same wallet concurrently once. No lock needed.
    _wallet_cache[wallet] = {"fp": fp, "result": result}
    return result


@app.route("/api/meta")
def api_meta():
    from collections import defaultdict
    db.init_db()
    with db.get_conn() as conn:
        fp = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(rowid), 0) FROM trades"
        ).fetchone()
        all_wallets = [r[0] for r in conn.execute(
            "SELECT DISTINCT wallet_address FROM trades"
        ).fetchall()]

    cache_key = (fp[0], fp[1])
    if (_meta_cache["payload"] is not None
            and _meta_cache["key"] == cache_key
            and _time.time() < _meta_cache["expires"]):
        return jsonify(_meta_cache["payload"])

    now = int(_time.time())
    cutoffs = {d: now - d * 86400 for d in META_WINDOWS if d}

    def _new_stats():
        return {
            "name": "", "buys": 0, "sells": 0,
            "buy_eth": 0.0, "sell_eth": 0.0, "fees_eth": 0.0,
            "realized_pnl": 0.0, "matched_trades": 0,
            "holding_times": [], "open_positions": 0, "total_fee_bps": 0,
            "wins": 0, "losses": 0,
            "first_trade_ts": None,
            "wallets": set(),
        }

    merged = {d: defaultdict(_new_stats) for d in META_WINDOWS}

    with db.get_conn() as conn:
        for wallet in all_wallets:
            result = get_cached_analytics(conn, wallet)
            if not result:
                continue

            for addr, s in result["per_collection"].items():
                # Metadata shared by every window bucket
                for d in META_WINDOWS:
                    m = merged[d][addr]
                    if s["name"]:
                        m["name"] = s["name"]
                    m["total_fee_bps"] = s["total_fee_bps"]
                    m["open_positions"] += s["open_positions"]
                    m["wallets"].add(wallet)
                    ft = s.get("first_trade_ts")
                    if ft and (m["first_trade_ts"] is None or ft < m["first_trade_ts"]):
                        m["first_trade_ts"] = ft

                # All-time bucket takes the full per-collection stats
                m = merged[0][addr]
                m["buys"] += s["buys"]
                m["sells"] += s["sells"]
                m["buy_eth"] += s["buy_eth"]
                m["sell_eth"] += s["sell_eth"]
                m["fees_eth"] += s["fees_eth"]
                m["realized_pnl"] += s["realized_pnl"]
                m["matched_trades"] += s["matched_trades"]
                m["holding_times"].extend(s.get("holding_times") or [])
                m["wins"] += s.get("wins", 0)
                m["losses"] += s.get("losses", 0)

            # Windowed round trips: dated by the sell, buy may predate the window
            for mt in result["matched_trades"]:
                for d, cutoff in cutoffs.items():
                    if mt["sell_ts"] < cutoff:
                        continue
                    m = merged[d][mt["collection_address"]]
                    m["sells"] += 1
                    m["buy_eth"] += mt["buy_eth"]
                    m["sell_eth"] += mt["sell_eth"]
                    m["fees_eth"] += mt["sell_fees_eth"]
                    m["realized_pnl"] += mt["pnl_eth"]
                    m["matched_trades"] += 1
                    if mt["holding_secs"] >= 0:
                        m["holding_times"].append(mt["holding_secs"])
                    if mt["pnl_eth"] > 0:
                        m["wins"] += 1
                    else:
                        m["losses"] += 1

        # Windowed buys: buy events inside the window, merged across wallets
        # (same as the old per-wallet Python loop, in one SQL pass per window)
        for d, cutoff in cutoffs.items():
            buy_rows = conn.execute(
                "SELECT collection_address, COUNT(*) FROM trades"
                " WHERE side = 'buy' AND block_timestamp >= ? GROUP BY collection_address",
                (cutoff,)
            ).fetchall()
            for addr, n in buy_rows:
                merged[d][addr]["buys"] += n

        ts_rows = conn.execute(
            "SELECT collection_address, MAX(block_timestamp), MIN(block_timestamp) FROM trades GROUP BY collection_address"
        ).fetchall()
        last_ts_map = {r[0]: r[1] for r in ts_rows}
        first_ts_map = {r[0]: r[2] for r in ts_rows}
        cutoff_7d = now - 7 * 86400
        recent_rows = conn.execute(
            "SELECT collection_address, COUNT(*) FROM trades WHERE block_timestamp >= ? GROUP BY collection_address",
            (cutoff_7d,)
        ).fetchall()
        trades_7d_map = {r[0]: r[1] for r in recent_rows}

    payload = {}
    for d in META_WINDOWS:
        rows = []
        for addr, s in merged[d].items():
            total_trades = s["buys"] + s["sells"]
            # Windowed views only list collections with a sale inside the window
            if d and s["sells"] == 0:
                continue
            if not total_trades:
                continue
            ht = s["holding_times"]
            avg_holding_secs = sum(ht) / len(ht) if ht else None
            roi_pct = (s["realized_pnl"] / s["buy_eth"] * 100) if s["buy_eth"] else 0
            total_matched = s["wins"] + s["losses"]
            win_rate = s["wins"] / total_matched if total_matched else 0
            rows.append({
                "address": addr,
                "name": s["name"] or addr[:10] + "...",
                "total_trades": total_trades,
                "matched_trades": s["matched_trades"],
                "buys": s["buys"],
                "sells": s["sells"],
                "buy_eth": round(s["buy_eth"], 4),
                "sell_eth": round(s["sell_eth"], 4),
                "fees_eth": round(s["fees_eth"], 4),
                "realized_pnl": round(s["realized_pnl"], 4),
                "roi_pct": round(roi_pct, 2),
                "avg_holding_secs": round(avg_holding_secs, 1) if avg_holding_secs is not None else None,
                "wins": s["wins"],
                "losses": s["losses"],
                "win_rate": round(win_rate * 100, 1),
                "open_positions": s["open_positions"],
                "total_fee_bps": s["total_fee_bps"],
                "last_trade_ts": last_ts_map.get(addr),
                "first_trade_ts": s.get("first_trade_ts") or first_ts_map.get(addr),
                "trades_7d": trades_7d_map.get(addr, 0),
                "wallets": list(s["wallets"]),
            })
        rows.sort(key=lambda r: r["roi_pct"], reverse=True)
        payload[str(d)] = rows

    _meta_cache.update(key=cache_key, expires=_time.time() + 300, payload=payload)
    return jsonify(payload)


# ── API: benchmark (wallet vs basket comparison) ──────────────────────────────

@app.route("/api/benchmark")
def api_benchmark():
    from collections import defaultdict
    import re as _re

    wallet_addr = request.args.get("wallet", "").strip().lower()
    days = request.args.get("days", 0, type=int)
    collection_filter = request.args.get("collection", "").strip().lower() or None

    if not wallet_addr or not _re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet_addr):
        return jsonify({"error": "Invalid wallet address"}), 400

    since = int(_time.time() - days * 86400) if days else None

    db.init_db()
    with db.get_conn() as conn:
        if since or collection_filter:
            target_trades = db.get_trades(conn, wallet_addr, since=since)
            if collection_filter:
                target_trades = [t for t in target_trades if t["collection_address"] == collection_filter]
            target_stats = analytics.compute_analytics(target_trades) if target_trades else {}
        else:
            target_stats = get_cached_analytics(conn, wallet_addr)

    if not target_stats:
        msg = "No trades in this time range." if since else "No trades found for this wallet."
        return jsonify({"error": msg}), 404

    target_collections = set(target_stats["per_collection"].keys())

    if not target_collections:
        return jsonify({"error": "No collection data found."}), 404

    # Get basket wallet addresses.
    # Default: ALL other wallets (full data, not restricted to shared collections).
    # Collection mode: wallets that traded that specific collection.
    with db.get_conn() as conn:
        if collection_filter:
            if since:
                basket_rows = conn.execute(
                    "SELECT DISTINCT wallet_address FROM trades WHERE wallet_address != ?"
                    " AND collection_address = ? AND block_timestamp >= ?",
                    [wallet_addr, collection_filter, since]
                ).fetchall()
            else:
                basket_rows = conn.execute(
                    "SELECT DISTINCT wallet_address FROM trades WHERE wallet_address != ?"
                    " AND collection_address = ?",
                    [wallet_addr, collection_filter]
                ).fetchall()
        else:
            if since:
                basket_rows = conn.execute(
                    "SELECT DISTINCT wallet_address FROM trades WHERE wallet_address != ?"
                    " AND block_timestamp >= ?",
                    [wallet_addr, since]
                ).fetchall()
            else:
                basket_rows = conn.execute(
                    "SELECT DISTINCT wallet_address FROM trades WHERE wallet_address != ?",
                    [wallet_addr]
                ).fetchall()

    basket_wallets = [r["wallet_address"] for r in basket_rows]

    # Aggregate basket stats.
    # basket_per_col: filtered to target_collections, used for per-collection table.
    # basket_all_*: all trades across all collections, used for summary cards.
    basket_per_col = defaultdict(lambda: {
        "buys": 0, "sells": 0, "buy_eth": 0.0, "sell_eth": 0.0, "fees_eth": 0.0,
        "realized_pnl": 0.0, "matched_trades": 0, "wins": 0, "losses": 0,
        "holding_times": [], "name": "", "wallet_count": 0,
    })
    basket_all_buy_eth = 0.0
    basket_all_pnl = 0.0
    basket_all_trades = 0
    basket_all_ht: list = []
    basket_all_wins = 0
    basket_all_losses = 0
    basket_all_open_count = 0
    basket_all_cost_basis_open = 0.0
    basket_open_data: list = []  # (cost, slug, fee_bps) per open position

    with db.get_conn() as conn:
        for bw in basket_wallets:
            if since or collection_filter:
                bw_trades = db.get_trades(conn, bw, since=since)
                if collection_filter:
                    bw_trades = [t for t in bw_trades if t["collection_address"] == collection_filter]
                bw_stats = analytics.compute_analytics(bw_trades) if bw_trades else {}
            else:
                bw_stats = get_cached_analytics(conn, bw)
            if not bw_stats:
                continue
            bs = bw_stats["summary"]

            # Full-basket summary accumulation (all their trades)
            basket_all_buy_eth += bs.get("total_buy_eth", 0)
            basket_all_pnl += bs.get("realized_pnl_eth", 0)
            basket_all_trades += bs.get("total_trades", 0)
            basket_all_open_count += bs.get("open_positions", 0)
            basket_all_cost_basis_open += bs.get("open_cost_basis_eth", 0)
            for _buys in (bw_stats.get("open_positions") or {}).values():
                for _b in _buys:
                    basket_open_data.append((
                        _b.get("eth_amount", 0) + (_b.get("gas_eth") or 0),
                        _b.get("collection_slug"),
                        _b.get("total_fee_bps") or 0,
                    ))

            for col_addr, cs in bw_stats["per_collection"].items():
                basket_all_wins += cs.get("wins", 0)
                basket_all_losses += cs.get("losses", 0)
                basket_all_ht.extend(cs.get("holding_times") or [])

                # Per-collection table: only accumulate for target wallet's collections
                if col_addr not in target_collections:
                    continue
                b = basket_per_col[col_addr]
                if cs["name"]:
                    b["name"] = cs["name"]
                b["buys"] += cs["buys"]
                b["sells"] += cs["sells"]
                b["buy_eth"] += cs["buy_eth"]
                b["sell_eth"] += cs["sell_eth"]
                b["fees_eth"] += cs["fees_eth"]
                b["realized_pnl"] += cs["realized_pnl"]
                b["matched_trades"] += cs["matched_trades"]
                b["wins"] += cs.get("wins", 0)
                b["losses"] += cs.get("losses", 0)
                b["holding_times"].extend(cs.get("holding_times") or [])
                b["wallet_count"] += 1

    # Build per-collection comparison list
    collections_out = []
    for col_addr in target_collections:
        tc = target_stats["per_collection"][col_addr]
        bc = basket_per_col.get(col_addr, {})

        t_ht = tc.get("holding_times") or []
        t_wins, t_losses = tc.get("wins", 0), tc.get("losses", 0)
        t_buy, t_pnl = tc.get("buy_eth", 0), tc.get("realized_pnl", 0)

        wallet_col = {
            "buys": tc["buys"],
            "sells": tc["sells"],
            "buy_eth": round(t_buy, 4),
            "roi": round(t_pnl / t_buy * 100, 2) if t_buy else None,
            "avg_holding_secs": round(sum(t_ht) / len(t_ht), 1) if t_ht else None,
            "realized_pnl": round(t_pnl, 4),
            "wins": t_wins,
            "losses": t_losses,
            "win_rate": round(t_wins / (t_wins + t_losses) * 100, 1) if (t_wins + t_losses) > 0 else None,
            "matched_trades": tc.get("matched_trades", 0),
        }

        if bc:
            bc_ht = bc.get("holding_times") or []
            bc_wins, bc_losses = bc.get("wins", 0), bc.get("losses", 0)
            bc_buy, bc_pnl = bc.get("buy_eth", 0), bc.get("realized_pnl", 0)
            basket_col = {
                "buys": bc.get("buys", 0),
                "sells": bc.get("sells", 0),
                "buy_eth": round(bc_buy, 4),
                "roi": round(bc_pnl / bc_buy * 100, 2) if bc_buy else None,
                "avg_holding_secs": round(sum(bc_ht) / len(bc_ht), 1) if bc_ht else None,
                "realized_pnl": round(bc_pnl, 4),
                "wins": bc_wins,
                "losses": bc_losses,
                "win_rate": round(bc_wins / (bc_wins + bc_losses) * 100, 1) if (bc_wins + bc_losses) > 0 else None,
                "matched_trades": bc.get("matched_trades", 0),
                "wallet_count": bc.get("wallet_count", 0),
            }
        else:
            basket_col = {
                "buys": 0, "sells": 0, "buy_eth": 0, "roi": None,
                "avg_holding_secs": None, "realized_pnl": 0,
                "wins": 0, "losses": 0, "win_rate": None, "matched_trades": 0, "wallet_count": 0,
            }

        collections_out.append({
            "address": col_addr,
            "name": tc.get("name") or bc.get("name") or col_addr[:10] + "...",
            "wallet": wallet_col,
            "basket": basket_col,
        })

    # Inventory / uPnL — cached floor prices only, no live fetches
    target_open_data = []
    for _buys in (target_stats.get("open_positions") or {}).values():
        for _b in _buys:
            target_open_data.append((
                _b.get("eth_amount", 0) + (_b.get("gas_eth") or 0),
                _b.get("collection_slug"),
                _b.get("total_fee_bps") or 0,
            ))

    _all_slugs = {slug for _, slug, _ in target_open_data + basket_open_data if slug}
    _cached_floors = {}
    if _all_slugs:
        with db.get_conn() as conn:
            _cached_floors = db.get_cached_floors(conn, list(_all_slugs))

    def _floor_upnl(open_data, cost_basis):
        floor_val = sum(
            _cached_floors[slug].get("floor_price_eth", 0) * (1 - fee_bps / 10000)
            for _, slug, fee_bps in open_data
            if slug and slug in _cached_floors and _cached_floors[slug].get("floor_price_eth")
        )
        return round(floor_val - cost_basis, 4) if floor_val > 0 else None

    s = target_stats["summary"]
    target_open_cost = s.get("open_cost_basis_eth", 0)
    target_upnl = _floor_upnl(target_open_data, target_open_cost)
    basket_upnl = _floor_upnl(basket_open_data, basket_all_cost_basis_open)

    # Target summary
    target_summary = {
        "total_trades": s["total_trades"],
        "total_buy_eth": round(s["total_buy_eth"], 4),
        "realized_pnl_eth": round(s["realized_pnl_eth"], 4),
        "roi": round(s["roi"] * 100, 2) if s.get("total_buy_eth") else None,
        "avg_holding_secs": round(s["avg_holding"], 1) if s.get("avg_holding") else None,
        "win_rate": round(s["win_rate"] * 100, 1) if s.get("win_rate") is not None else None,
        "open_positions": s.get("open_positions", 0),
        "open_cost_basis_eth": round(target_open_cost, 4),
        "upnl_eth": target_upnl,
    }

    # Basket summary — uses all trades across all collections (full data)
    basket_summary = {
        "total_trades": basket_all_trades,
        "total_buy_eth": round(basket_all_buy_eth, 4),
        "realized_pnl_eth": round(basket_all_pnl, 4),
        "roi": round(basket_all_pnl / basket_all_buy_eth * 100, 2) if basket_all_buy_eth else None,
        "avg_holding_secs": round(sum(basket_all_ht) / len(basket_all_ht), 1) if basket_all_ht else None,
        "win_rate": round(basket_all_wins / (basket_all_wins + basket_all_losses) * 100, 1)
                    if (basket_all_wins + basket_all_losses) > 0 else None,
        "wallet_count": len(basket_wallets),
        "open_positions": basket_all_open_count,
        "open_cost_basis_eth": round(basket_all_cost_basis_open, 4),
        "upnl_eth": basket_upnl,
    }

    with db.get_conn() as conn:
        wallet_row = db.get_wallet(conn, wallet_addr)

    return jsonify({
        "wallet": {
            "address": wallet_addr,
            "name": wallet_row["name"] if wallet_row else None,
        },
        "timeframe_days": days,
        "collection_filter": collection_filter,
        "target_summary": target_summary,
        "basket_summary": basket_summary,
        "collections": collections_out,
    })


# ── API: collection detail (per-wallet breakdown for one collection) ───────────

@app.route("/api/collection/<address>")
def api_collection_detail(address):
    address = address.lower()
    db.init_db()
    with db.get_conn() as conn:
        wallets = [r[0] for r in conn.execute(
            "SELECT DISTINCT wallet_address FROM trades WHERE collection_address = ?",
            (address,)
        ).fetchall()]
        col_row = conn.execute(
            "SELECT name, slug FROM collections WHERE contract_address = ?",
            (address,)
        ).fetchone()

    col_name = (col_row["name"] or col_row["slug"] if col_row else None) or address[:10] + "..."

    rows = []
    with db.get_conn() as conn:
        for wallet in wallets:
            result = get_cached_analytics(conn, wallet)
            if not result:
                continue
            s = result["per_collection"].get(address)
            if not s:
                continue
            wallet_row = db.get_wallet(conn, wallet)
            total_trades = s["buys"] + s["sells"]
            roi = s["roi"] * 100 if s.get("roi") is not None else None
            rows.append({
                "wallet_address": wallet,
                "wallet_name": wallet_row["name"] if wallet_row else None,
                "trades": total_trades,
                "buys": s["buys"],
                "sells": s["sells"],
                "realized_pnl": round(s["realized_pnl"], 4),
                "roi_pct": round(roi, 2) if roi is not None else None,
                "first_trade_ts": s.get("first_trade_ts"),
                "last_trade_ts": s.get("last_trade_ts") or 0,
            })

    rows.sort(key=lambda r: r["trades"], reverse=True)
    return jsonify({"collection_address": address, "collection_name": col_name, "wallets": rows})


@app.route("/api/collection_pnl_buckets/<address>")
def api_collection_pnl_buckets(address):
    address = address.lower()
    since = request.args.get("since", type=int, default=0)
    db.init_db()
    with db.get_conn() as conn:
        wallets = [r[0] for r in conn.execute(
            "SELECT DISTINCT wallet_address FROM trades WHERE collection_address = ?",
            (address,)
        ).fetchall()]

    buckets_map = {}
    with db.get_conn() as conn:
        for wallet in wallets:
            if since:
                trades = db.get_trades(conn, wallet, since=since)
                result = analytics.compute_analytics(trades) if trades else {}
            else:
                result = get_cached_analytics(conn, wallet)
            for m in result.get("matched_trades", []):
                if m["collection_address"] != address:
                    continue
                _add_daily_bucket(buckets_map, m)

    buckets = sorted(buckets_map.values(), key=lambda b: b["key"])
    total_pnl = sum(b["pnl_eth"] for b in buckets)
    return jsonify({"buckets": buckets, "bucket_type": "daily", "total_pnl_eth": total_pnl})


# ── API: collections list (for graph picker) ───────────────────────────────────

@app.route("/api/collections")
def api_collections():
    db.init_db()
    with db.get_conn() as conn:
        rows = conn.execute("""
            SELECT c.contract_address, c.slug, c.name,
                   COUNT(t.id) AS trade_count
            FROM collections c
            JOIN trades t ON t.collection_address = c.contract_address
            GROUP BY c.contract_address
            ORDER BY trade_count DESC, c.name
        """).fetchall()
    return jsonify([dict(r) for r in rows])


# ── API: sell graph data (matched round-trips per collection) ──────────────────

@app.route("/api/sell_graph")
def api_sell_graph():
    slugs = [s.strip() for s in request.args.get("slugs", "").split(",") if s.strip()]
    addrs = [a.strip().lower() for a in request.args.get("addrs", "").split(",") if a.strip()]

    if not slugs and not addrs:
        return jsonify({"error": "Provide slugs or addrs param"}), 400

    db.init_db()
    with db.get_conn() as conn:
        all_wallets = [r[0] for r in conn.execute(
            "SELECT DISTINCT wallet_address FROM trades"
        ).fetchall()]

        # Resolve addr→info and slug→info for all requested collections
        col_info = {}
        for slug in slugs:
            row = conn.execute(
                "SELECT contract_address, name, slug FROM collections WHERE slug = ?", (slug,)
            ).fetchone()
            if row:
                col_info[row["contract_address"]] = dict(row)
        for addr in addrs:
            row = conn.execute(
                "SELECT contract_address, name, slug FROM collections WHERE contract_address = ?", (addr,)
            ).fetchone()
            if row:
                col_info[addr] = dict(row)
            elif addr not in col_info:
                col_info[addr] = {"contract_address": addr, "name": addr[:10] + "...", "slug": ""}

    all_sells = []

    for wallet in all_wallets:
        with db.get_conn() as conn:
            if slugs and addrs:
                sp = ",".join("?" * len(slugs))
                ap = ",".join("?" * len(addrs))
                trades = conn.execute(f"""
                    SELECT t.*, c.name AS collection_name, c.total_fee_bps
                    FROM trades t LEFT JOIN collections c ON t.collection_address = c.contract_address
                    WHERE t.wallet_address = ?
                      AND (t.collection_slug IN ({sp}) OR t.collection_address IN ({ap}))
                    ORDER BY t.block_timestamp ASC
                """, [wallet] + slugs + addrs).fetchall()
            elif slugs:
                sp = ",".join("?" * len(slugs))
                trades = conn.execute(f"""
                    SELECT t.*, c.name AS collection_name, c.total_fee_bps
                    FROM trades t LEFT JOIN collections c ON t.collection_address = c.contract_address
                    WHERE t.wallet_address = ? AND t.collection_slug IN ({sp})
                    ORDER BY t.block_timestamp ASC
                """, [wallet] + slugs).fetchall()
            else:
                ap = ",".join("?" * len(addrs))
                trades = conn.execute(f"""
                    SELECT t.*, c.name AS collection_name, c.total_fee_bps
                    FROM trades t LEFT JOIN collections c ON t.collection_address = c.contract_address
                    WHERE t.wallet_address = ? AND t.collection_address IN ({ap})
                    ORDER BY t.block_timestamp ASC
                """, [wallet] + addrs).fetchall()

        if not trades:
            continue

        result = analytics.compute_analytics(trades)
        for m in result.get("matched_trades", []):
            col_addr = m["collection_address"]
            info = col_info.get(col_addr, {})
            buy_eth = m["buy_eth"]
            sell_eth = m["sell_eth"]
            roi_pct = (sell_eth / buy_eth - 1) * 100 if buy_eth else 0
            all_sells.append({
                "ts": m["sell_ts"],
                "buy_eth": round(buy_eth, 4),
                "sell_eth": round(sell_eth, 4),
                "roi_pct": round(roi_pct, 2),
                "pnl_eth": round(m["pnl_eth"], 4),
                "sell_type": m.get("sell_type"),
                "collection_addr": col_addr,
                "collection_slug": info.get("slug") or m.get("collection_slug", ""),
                "collection_name": info.get("name") or m.get("collection_name", col_addr[:10]),
                "nft_id": m["nft_id"],
                "wallet": wallet,
            })

    all_sells.sort(key=lambda x: x["ts"])

    return jsonify({
        "sells": all_sells,
        "collections": [
            {"addr": addr, "slug": info.get("slug", ""), "name": info.get("name", addr[:10])}
            for addr, info in col_info.items()
        ],
    })


# ── API: Dune top traders ─────────────────────────────────────────────────────

DUNE_QUERY_ID = 7785187

@app.route("/api/dune/top_traders")
def api_dune_top_traders():
    days  = request.args.get("days",  "30")
    limit = request.args.get("limit", "100")

    dune_key = os.getenv("DUNE_API_KEY")
    if not dune_key:
        return jsonify({"error": "DUNE_API_KEY not set"}), 500

    hdrs = {"X-Dune-API-Key": dune_key}

    # Trigger fresh execution with the given parameters
    exec_resp = _req.post(
        f"https://api.dune.com/api/v1/query/{DUNE_QUERY_ID}/execute",
        headers=hdrs,
        json={"query_parameters": {"Number of Days": days, "Top X Traders": limit}},
        timeout=15,
    )
    if not exec_resp.ok:
        return jsonify({"error": f"Dune execute failed: {exec_resp.text}"}), 502

    execution_id = exec_resp.json()["execution_id"]

    # Poll until complete (max 60s)
    for _ in range(60):
        status_resp = _req.get(
            f"https://api.dune.com/api/v1/execution/{execution_id}/status",
            headers=hdrs,
            timeout=10,
        )
        state = status_resp.json().get("state", "")
        if state == "QUERY_STATE_COMPLETED":
            break
        if any(s in state for s in ("FAILED", "CANCELLED", "EXPIRED")):
            return jsonify({"error": f"Dune query {state}"}), 500
        _time.sleep(1)
    else:
        return jsonify({"error": "Dune query timed out after 60s"}), 504

    result_resp = _req.get(
        f"https://api.dune.com/api/v1/execution/{execution_id}/results?limit={limit}",
        headers=hdrs,
        timeout=15,
    )
    data = result_resp.json()
    rows = data.get("result", {}).get("rows", [])
    meta = data.get("result", {}).get("metadata", {})
    return jsonify({"rows": rows, "total": meta.get("total_row_count", len(rows))})


if __name__ == "__main__":
    db.init_db()
    app.run(debug=True, port=5000, threaded=True, use_reloader=False)
