"""Flask web frontend for NFT Player Analysis."""

import json
import os
import subprocess
import sys
import time as _time
from datetime import datetime, timezone as _tz

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

@app.route("/api/wallets")
def api_wallets():
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
        sell_rows = conn.execute("""
            SELECT wallet_address, block_timestamp
            FROM trades WHERE side = 'sell'
            ORDER BY wallet_address, block_timestamp
        """).fetchall()

    sell_map = {}
    for r in sell_rows:
        sell_map.setdefault(r["wallet_address"], []).append(r["block_timestamp"])

    result = []
    for r in rows:
        d = dict(r)
        d["sell_timestamps"] = sell_map.get(d["address"], [])
        result.append(d)
    return jsonify(result)


# ── API: report ────────────────────────────────────────────────────────────────

@app.route("/api/report/<address>")
def api_report(address):
    address = address.lower()
    since = request.args.get("since", type=int)
    db.init_db()
    with db.get_conn() as conn:
        trades = db.get_trades(conn, address, since=since)
        wallet_row = db.get_wallet(conn, address)
        sync_state = db.get_sync_state(conn, address)
        latest_trade_ts = db.get_latest_trade_ts(conn, address)

    if not trades:
        msg = "No trades in this time range." if since else "No trades found. Run a sync first."
        return jsonify({"error": msg}), 404

    result = analytics.compute_analytics(trades)

    # Only update the all-time summary cache when not filtered
    if not since:
        with db.get_conn() as conn:
            db.upsert_wallet_summary(conn, address, result["summary"], latest_trade_ts)

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
        trades = db.get_trades(conn, address, since=since if since else None)
    if not trades:
        return jsonify({"buckets": [], "bucket_type": "monthly", "total_pnl_eth": 0})

    result = analytics.compute_analytics(trades)
    matched = result.get("matched_trades", [])

    now = int(_time.time())
    range_days = (now - since) / 86400 if since else 99999
    if range_days > 91:
        bucket_type = "monthly"
    else:
        bucket_type = "daily"

    buckets_map = {}
    for m in matched:
        dt = datetime.fromtimestamp(m["sell_ts"], tz=_tz.utc)
        if bucket_type == "monthly":
            key = dt.strftime("%Y-%m")
            label = dt.strftime("%b '") + dt.strftime("%y")
        else:
            key = dt.strftime("%Y-%m-%d")
            label = dt.strftime("%b ") + str(dt.day)
        if key not in buckets_map:
            buckets_map[key] = {"key": key, "label": label, "pnl_eth": 0.0, "trade_count": 0}
        buckets_map[key]["pnl_eth"] += m["pnl_eth"]
        buckets_map[key]["trade_count"] += 1

    buckets = sorted(buckets_map.values(), key=lambda b: b["key"])
    total_pnl = sum(b["pnl_eth"] for b in buckets)
    return jsonify({"buckets": buckets, "bucket_type": bucket_type, "total_pnl_eth": total_pnl})


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


# ── API: meta analysis (cross-wallet, collections with >10 trades) ─────────────

@app.route("/api/meta")
def api_meta():
    from collections import defaultdict
    db.init_db()
    with db.get_conn() as conn:
        all_wallets = [r[0] for r in conn.execute(
            "SELECT DISTINCT wallet_address FROM trades"
        ).fetchall()]

    merged = defaultdict(lambda: {
        "name": "", "buys": 0, "sells": 0,
        "buy_eth": 0.0, "sell_eth": 0.0, "fees_eth": 0.0,
        "realized_pnl": 0.0, "matched_trades": 0,
        "holding_times": [], "open_positions": 0, "total_fee_bps": 0,
        "wins": 0, "losses": 0,
        "first_trade_ts": None,
    })

    for wallet in all_wallets:
        with db.get_conn() as conn:
            trades = db.get_trades(conn, wallet)
        if not trades:
            continue
        result = analytics.compute_analytics(trades)
        for addr, s in result["per_collection"].items():
            m = merged[addr]
            if s["name"]:
                m["name"] = s["name"]
            m["buys"] += s["buys"]
            m["sells"] += s["sells"]
            m["buy_eth"] += s["buy_eth"]
            m["sell_eth"] += s["sell_eth"]
            m["fees_eth"] += s["fees_eth"]
            m["realized_pnl"] += s["realized_pnl"]
            m["matched_trades"] += s["matched_trades"]
            m["holding_times"].extend(s.get("holding_times") or [])
            m["open_positions"] += s["open_positions"]
            m["total_fee_bps"] = s["total_fee_bps"]
            m["wins"] += s.get("wins", 0)
            m["losses"] += s.get("losses", 0)
            ft = s.get("first_trade_ts")
            if ft and (m["first_trade_ts"] is None or ft < m["first_trade_ts"]):
                m["first_trade_ts"] = ft

    with db.get_conn() as conn:
        ts_rows = conn.execute(
            "SELECT collection_address, MAX(block_timestamp), MIN(block_timestamp) FROM trades GROUP BY collection_address"
        ).fetchall()
        last_ts_map = {r[0]: r[1] for r in ts_rows}
        first_ts_map = {r[0]: r[2] for r in ts_rows}
        cutoff_7d = int(_time.time()) - 7 * 86400
        recent_rows = conn.execute(
            "SELECT collection_address, COUNT(*) FROM trades WHERE block_timestamp >= ? GROUP BY collection_address",
            (cutoff_7d,)
        ).fetchall()
        trades_7d_map = {r[0]: r[1] for r in recent_rows}

    rows = []
    for addr, s in merged.items():
        total_trades = s["buys"] + s["sells"]
        if total_trades <= 10:
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
        })

    rows.sort(key=lambda r: r["roi_pct"], reverse=True)
    return jsonify(rows)


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
    for wallet in wallets:
        with db.get_conn() as conn:
            trades = db.get_trades(conn, wallet)
            wallet_row = db.get_wallet(conn, wallet)
        if not trades:
            continue
        result = analytics.compute_analytics(trades)
        s = result["per_collection"].get(address)
        if not s:
            continue
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
