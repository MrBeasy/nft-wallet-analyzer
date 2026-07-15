"""Flask web frontend for NFT Player Analysis."""

import json
import logging
import os
import subprocess
import sys
import time as _time
from collections import defaultdict
from datetime import datetime, timedelta, timezone as _tz

log = logging.getLogger(__name__)

import requests as _req
from flask import Flask, Response, jsonify, render_template, request, stream_with_context

import analytics
import db
import fetch

app = Flask(__name__)

MARKET_BACKFILL_DAYS = 30
MARKET_SYNC_MIN_INTERVAL = 600  # skip re-sync if fresh within 10 min


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
    sell_from  = request.args.get("sell_from",  type=int)
    sell_until = request.args.get("sell_until", type=int)
    db.init_db()
    with db.get_conn() as conn:
        rows = conn.execute("""
            SELECT w.address, w.name, w.notes, w.entity_id,
                   s.last_synced_at, s.full_sync_complete,
                   ws.realized_pnl_eth, ws.total_trades, ws.win_rate,
                   ws.collections_traded, ws.computed_at, ws.open_positions,
                   ws.avg_holding_secs, ws.total_buy_eth
            FROM wallets w
            LEFT JOIN sync_state s        ON w.address = s.wallet_address
            LEFT JOIN wallet_summaries ws ON w.address = ws.wallet_address
            ORDER BY ws.realized_pnl_eth DESC, w.name
        """).fetchall()

        entity_rows = conn.execute("""
            SELECT e.id, e.name, e.notes,
                   es.realized_pnl_eth, es.total_trades, es.win_rate,
                   es.collections_traded, es.computed_at, es.open_positions,
                   es.avg_holding_secs, es.total_buy_eth, es.latest_trade_ts,
                   es.wash_legs, es.wash_cost_eth
            FROM entities e
            LEFT JOIN entity_summaries es ON e.id = es.entity_id
        """).fetchall()
        entity_map = {e["id"]: dict(e) for e in entity_rows}

        result = []
        seen_entities = set()

        for r in rows:
            d = dict(r)
            eid = d.get("entity_id")

            if eid is not None and eid not in seen_entities:
                seen_entities.add(eid)

                e = entity_map.get(eid, {})
                members = db.get_entity_members(conn, eid)

                # Self-heal: recompute summary if trades have been added since last compute
                latest_ts = db.get_latest_trade_ts_multi(conn, members) if members else 0
                if members and (not e.get("latest_trade_ts") or e["latest_trade_ts"] < latest_ts):
                    er = get_cached_entity_analytics(conn, eid)
                    if er and er.get("summary"):
                        db.upsert_entity_summary(conn, eid, er["summary"], latest_ts)
                        s = er["summary"]
                        e.update({
                            "realized_pnl_eth": s["realized_pnl_eth"],
                            "total_trades": s["total_trades"],
                            "win_rate": s["win_rate"],
                            "collections_traded": s["collections_traded"],
                            "computed_at": None,
                            "open_positions": s["open_positions"],
                            "avg_holding_secs": s.get("avg_holding") or 0,
                            "total_buy_eth": s["total_buy_eth"],
                            "wash_legs": s.get("wash_legs", 0),
                            "wash_cost_eth": s.get("wash_cost_eth", 0.0),
                            "latest_trade_ts": latest_ts,
                        })

                last_synced = None
                full_sync = 1
                for addr in members:
                    ss = db.get_sync_state(conn, addr)
                    if ss and ss["last_synced_at"]:
                        if last_synced is None or ss["last_synced_at"] < last_synced:
                            last_synced = ss["last_synced_at"]
                    if not ss or not ss["full_sync_complete"]:
                        full_sync = 0

                cluster = {
                    "address": None,
                    "entity_id": eid,
                    "name": e.get("name"),
                    "notes": e.get("notes"),
                    "member_count": len(members),
                    "members": members,
                    "last_synced_at": last_synced,
                    "full_sync_complete": full_sync,
                    "realized_pnl_eth": e.get("realized_pnl_eth"),
                    "total_trades": e.get("total_trades"),
                    "win_rate": e.get("win_rate"),
                    "collections_traded": e.get("collections_traded"),
                    "computed_at": e.get("computed_at"),
                    "open_positions": e.get("open_positions"),
                    "avg_holding_secs": e.get("avg_holding_secs"),
                    "total_buy_eth": e.get("total_buy_eth"),
                    "wash_legs": e.get("wash_legs"),
                    "wash_cost_eth": e.get("wash_cost_eth"),
                }

                if sell_from is not None or sell_until is not None:
                    er = get_cached_entity_analytics(conn, eid)
                    matched = [
                        m for m in er.get("matched_trades", [])
                        if (sell_from  is None or m["sell_ts"] >= sell_from)
                        and (sell_until is None or m["sell_ts"] <= sell_until)
                    ]
                    cluster.update(_wallet_stats_from_matched(matched))

                result.append(cluster)

            # Always emit the individual wallet row (whether or not it's in a cluster)
            d["member_count"] = 1
            if sell_from is not None or sell_until is not None:
                ar = get_cached_analytics(conn, d["address"])
                matched = [
                    m for m in ar.get("matched_trades", [])
                    if (sell_from  is None or m["sell_ts"] >= sell_from)
                    and (sell_until is None or m["sell_ts"] <= sell_until)
                ]
                d.update(_wallet_stats_from_matched(matched))
            result.append(d)

    result.sort(key=lambda x: (
        -(x["realized_pnl_eth"]) if x.get("realized_pnl_eth") is not None else float("inf"),
        x.get("name") or ""
    ))
    return jsonify(result)


@app.route("/api/wallets/all")
def api_wallets_all():
    """All individual wallets with entity membership info. Used for cluster modal."""
    db.init_db()
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT address, name, entity_id FROM wallets ORDER BY name, address"
        ).fetchall()
        ents = {e["id"]: e["name"] for e in conn.execute(
            "SELECT id, name FROM entities"
        ).fetchall()}
    return jsonify([{
        "address": r["address"],
        "name": r["name"],
        "entity_id": r["entity_id"],
        "entity_name": ents.get(r["entity_id"]) if r["entity_id"] else None,
    } for r in rows])


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
        trades, result["per_collection"], result["summary"], floor_data,
        result["open_positions"]
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
    collection = (request.args.get("collection") or "").strip().lower()
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
        if collection and m["collection_address"] != collection:
            continue
        _add_daily_bucket(buckets_map, m)

    buckets = sorted(buckets_map.values(), key=lambda b: b["key"])
    total_pnl = sum(b["pnl_eth"] for b in buckets)
    with db.get_conn() as conn:
        ss = db.get_sync_state(conn, address)
    synced_at = ss["last_synced_at"] if ss else None
    return jsonify({"buckets": buckets, "bucket_type": "daily", "total_pnl_eth": total_pnl,
                    "synced_at": synced_at})


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


# ── API: entity CRUD ───────────────────────────────────────────────────────────

@app.route("/api/entities", methods=["POST"])
def api_entities_create():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    members = [a.lower() for a in (data.get("members") or [])]
    notes = data.get("notes")
    if not name:
        return jsonify({"error": "name is required"}), 400
    if not members:
        return jsonify({"error": "at least one member wallet is required"}), 400
    db.init_db()
    with db.get_conn() as conn:
        for addr in members:
            w = db.get_wallet(conn, addr)
            if not w:
                return jsonify({"error": f"Wallet {addr} not found"}), 400
            if w["entity_id"] is not None:
                return jsonify({"error": f"Wallet {addr} already belongs to another entity"}), 400
        entity_id = db.create_entity(conn, name, notes)
        db.set_entity_members(conn, entity_id, members)
        latest_ts = db.get_latest_trade_ts_multi(conn, members)
        er = get_cached_entity_analytics(conn, entity_id)
        if er and er.get("summary"):
            db.upsert_entity_summary(conn, entity_id, er["summary"], latest_ts)
    return jsonify({"id": entity_id}), 201


@app.route("/api/entities/<int:entity_id>", methods=["PATCH"])
def api_entity_update(entity_id):
    data = request.get_json() or {}
    name = data.get("name")
    notes = data.get("notes")
    members = data.get("members")
    db.init_db()
    with db.get_conn() as conn:
        entity = db.get_entity(conn, entity_id)
        if not entity:
            return jsonify({"error": "Entity not found"}), 404
        if members is not None:
            members = [a.lower() for a in members]
            if not members:
                return jsonify({"error": "members cannot be empty; use DELETE to dissolve"}), 400
            for addr in members:
                w = db.get_wallet(conn, addr)
                if not w:
                    return jsonify({"error": f"Wallet {addr} not found"}), 400
                if w["entity_id"] is not None and w["entity_id"] != entity_id:
                    return jsonify({"error": f"Wallet {addr} already belongs to another entity"}), 400
            db.set_entity_members(conn, entity_id, members)
            _entity_cache.pop(entity_id, None)
            latest_ts = db.get_latest_trade_ts_multi(conn, members)
            er = get_cached_entity_analytics(conn, entity_id)
            if er and er.get("summary"):
                db.upsert_entity_summary(conn, entity_id, er["summary"], latest_ts)
        if name is not None or notes is not None:
            db.update_entity(conn, entity_id, name=name, notes=notes)
    return jsonify({"ok": True})


@app.route("/api/entities/<int:entity_id>", methods=["DELETE"])
def api_entity_delete(entity_id):
    db.init_db()
    with db.get_conn() as conn:
        entity = db.get_entity(conn, entity_id)
        if not entity:
            return jsonify({"error": "Entity not found"}), 404
        db.delete_entity(conn, entity_id)
    _entity_cache.pop(entity_id, None)
    return jsonify({"ok": True})


# ── API: entity report ─────────────────────────────────────────────────────────

@app.route("/api/report/entity/<int:entity_id>")
def api_entity_report(entity_id):
    since = request.args.get("since", type=int)
    db.init_db()
    with db.get_conn() as conn:
        entity = db.get_entity(conn, entity_id)
        if not entity:
            return jsonify({"error": "Entity not found"}), 404
        members = db.get_entity_members(conn, entity_id)
        if not members:
            return jsonify({"error": "Entity has no members"}), 404
        member_rows = db.get_entity_member_rows(conn, entity_id)
        trades = db.get_trades_multi(conn, members, since=since)
        latest_trade_ts = db.get_latest_trade_ts_multi(conn, members)
        if not trades:
            msg = "No trades in this time range." if since else "No trades found. Sync member wallets first."
            return jsonify({"error": msg}), 404
        if since:
            result = analytics.compute_entity_analytics(trades, set(members))
        else:
            result = get_cached_entity_analytics(conn, entity_id)
            if result.get("summary"):
                db.upsert_entity_summary(conn, entity_id, result["summary"], latest_trade_ts)
        min_synced = None
        for addr in members:
            ss = db.get_sync_state(conn, addr)
            if ss and ss["last_synced_at"]:
                if min_synced is None or ss["last_synced_at"] < min_synced:
                    min_synced = ss["last_synced_at"]

    member_set = set(members)
    clean_trades = [t for t in trades if not (
        t["buyer_address"] in member_set and t["seller_address"] in member_set
    )]
    addr_to_slug = {}
    for t in clean_trades:
        if t["collection_slug"] and t["collection_address"]:
            addr_to_slug[t["collection_address"]] = t["collection_slug"]
    floor_data = {}
    if addr_to_slug:
        with db.get_conn() as conn2:
            floor_data = db.get_cached_floors(conn2, list(addr_to_slug.values()))
    player_card = analytics.compute_player_card(
        clean_trades, result["per_collection"], result["summary"], floor_data,
        result.get("open_positions")
    )
    per_col = {addr: {k: v for k, v in s.items() if k != "holding_times"}
               for addr, s in result["per_collection"].items()}
    return jsonify({
        "wallet": {
            "address": None,
            "entity_id": entity_id,
            "name": entity["name"],
            "notes": entity["notes"],
            "members": member_rows,
        },
        "summary": result["summary"],
        "per_collection": per_col,
        "open_positions": result.get("open_positions", {}),
        "sync_state": {"last_synced_at": min_synced},
        "filter_since": since,
        "player_card": player_card,
    })


# ── API: entity trades ─────────────────────────────────────────────────────────

@app.route("/api/trades/entity/<int:entity_id>")
def api_entity_trades(entity_id):
    since = request.args.get("since", type=int)
    collection = request.args.get("collection", "").strip()
    db.init_db()
    with db.get_conn() as conn:
        entity = db.get_entity(conn, entity_id)
        if not entity:
            return jsonify({"error": "Entity not found"}), 404
        members = db.get_entity_members(conn, entity_id)
        trades = db.get_trades_multi(conn, members, since=since)
    member_set = set(members)
    result = []
    for t in reversed(trades):  # DESC by timestamp
        d = dict(t)
        if collection and d["collection_address"] != collection and d.get("collection_slug") != collection:
            continue
        d["is_wash"] = (
            d.get("buyer_address") in member_set and
            d.get("seller_address") in member_set
        )
        result.append(d)
    return jsonify(result)


# ── API: entity PnL buckets ────────────────────────────────────────────────────

@app.route("/api/pnl_buckets/entity/<int:entity_id>")
def api_entity_pnl_buckets(entity_id):
    since = request.args.get("since", type=int, default=0)
    db.init_db()
    with db.get_conn() as conn:
        entity = db.get_entity(conn, entity_id)
        if not entity:
            return jsonify({"error": "Entity not found"}), 404
        members = db.get_entity_members(conn, entity_id)
        if since:
            trades = db.get_trades_multi(conn, members, since=since)
            result = analytics.compute_entity_analytics(trades, set(members)) if trades else {}
        else:
            result = get_cached_entity_analytics(conn, entity_id)
        sync_times = [db.get_sync_state(conn, m) for m in members]
    if not result:
        return jsonify({"buckets": [], "bucket_type": "daily", "total_pnl_eth": 0})
    buckets_map = {}
    for m in result.get("matched_trades", []):
        _add_daily_bucket(buckets_map, m)
    buckets = sorted(buckets_map.values(), key=lambda b: b["key"])
    total_pnl = sum(b["pnl_eth"] for b in buckets)
    synced_at = min((s["last_synced_at"] for s in sync_times if s and s["last_synced_at"]), default=None)
    return jsonify({"buckets": buckets, "bucket_type": "daily",
                    "total_pnl_eth": total_pnl, "synced_at": synced_at})


# ── API: floor prices + unrealized PnL ────────────────────────────────────────

def _fetch_floor_upnl(conn, open_positions: dict) -> dict:
    """Fetch/cache floor prices and compute uPnL for an open_positions dict.

    Returns the same payload shape as /api/floor responses.
    """
    import time as _time

    slug_to_fee = {}
    for buys in open_positions.values():
        for b in buys:
            slug = b.get("collection_slug")
            if slug:
                slug_to_fee[slug] = b.get("total_fee_bps") or 0

    now = int(_time.time())
    stale_threshold = now - db.FLOOR_CACHE_TTL_SECS
    cached = db.get_cached_floors(conn, list(slug_to_fee.keys()))

    stale_slugs = [
        slug for slug in slug_to_fee
        if slug not in cached
        or cached[slug]["floor_fetched_at"] is None
        or cached[slug]["floor_fetched_at"] < stale_threshold
    ]

    stale_set = set(stale_slugs)
    floor_prices = {}
    bid_prices = {}
    for slug, row in cached.items():
        if slug not in stale_set:
            if row["floor_price_eth"] is not None:
                floor_prices[slug] = row["floor_price_eth"]
            if row["best_offer_eth"] is not None:
                bid_prices[slug] = row["best_offer_eth"]

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
    return {
        "upnl_eth": upnl,
        "upnl_bid_eth": upnl_bid,
        "floor_value_eth": total_floor_net if positions_with_floor else None,
        "bid_value_eth": total_bid_net if positions_with_bid else None,
        "cost_basis_eth": total_cost,
        "floor_prices": floor_prices,
        "positions_with_floor": positions_with_floor,
        "positions_with_bid": positions_with_bid,
        "total_open": sum(len(v) for v in open_positions.values()),
    }


@app.route("/api/floor/<address>")
def api_floor(address):
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

        return jsonify(_fetch_floor_upnl(conn, open_positions))


@app.route("/api/floor/entity/<int:entity_id>")
def api_floor_entity(entity_id):
    db.init_db()
    with db.get_conn() as conn:
        entity = db.get_entity(conn, entity_id)
        if not entity:
            return jsonify({"error": "Entity not found"}), 404
        members = db.get_entity_members(conn, entity_id)
        if not members:
            return jsonify({"error": "Entity has no members"}), 404

        result = get_cached_entity_analytics(conn, entity_id)
        open_positions = result.get("open_positions", {})

        if not open_positions:
            return jsonify({"upnl_eth": None, "floor_value_eth": None,
                            "cost_basis_eth": 0, "floor_prices": {}})

        # Build union of all held NFTs across members.
        held_nfts: set = set()
        fetch_failed = False
        for addr in members:
            try:
                held_nfts |= fetch.fetch_wallet_nfts(addr)
            except Exception as e:
                log.warning("Could not fetch holdings for %s: %s", addr, e)
                fetch_failed = True

        if not fetch_failed:
            open_positions = {
                k: buys for k, buys in open_positions.items()
                if (k.split(":", 1)[0], k.split(":", 1)[1]) in held_nfts
            }

        if not open_positions:
            return jsonify({"upnl_eth": None, "floor_value_eth": None,
                            "cost_basis_eth": 0, "floor_prices": {},
                            "transferred_away": True})

        return jsonify(_fetch_floor_upnl(conn, open_positions))


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
# NOT caught (cosmetic only).
# Memory: ~23 wallets x full matched-trade/open-position dicts is tens of MB —
# fine for a local single-user tool.
_wallet_cache = {}  # wallet -> {"fp": (...), "result": analytics_dict}
_entity_cache = {}  # entity_id -> {"fp": (...), "result": analytics_dict}


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


def get_cached_entity_analytics(conn, entity_id):
    """Full-history entity analytics, cached. Fingerprint includes sorted member list."""
    members = db.get_entity_members(conn, entity_id)
    if not members:
        return analytics.compute_entity_analytics([], set())
    members_fp = tuple(sorted(members))
    ph = ",".join("?" * len(members))
    trades_fp = tuple(conn.execute(
        f"SELECT COUNT(*), COALESCE(MAX(id), 0), TOTAL(gas_eth) FROM trades WHERE wallet_address IN ({ph})",
        members
    ).fetchone())
    cols_fp = tuple(conn.execute(
        "SELECT COUNT(*), COALESCE(MAX(fetched_at), 0), TOTAL(total_fee_bps) FROM collections"
    ).fetchone())
    fp = (members_fp,) + trades_fp + cols_fp
    hit = _entity_cache.get(entity_id)
    if hit and hit["fp"] == fp:
        return hit["result"]
    trades = db.get_trades_multi(conn, members)
    result = analytics.compute_entity_analytics(trades, set(members))
    _entity_cache[entity_id] = {"fp": fp, "result": result}
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
    vs_addr = request.args.get("vs", "").strip().lower() or None
    days = request.args.get("days", 0, type=int)
    collection_filter = request.args.get("collection", "").strip().lower() or None

    if not wallet_addr or not _re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet_addr):
        return jsonify({"error": "Invalid wallet address"}), 400
    if vs_addr:
        if not _re.fullmatch(r"0x[0-9a-fA-F]{40}", vs_addr):
            return jsonify({"error": "Invalid opponent wallet address"}), 400
        if vs_addr == wallet_addr:
            return jsonify({"error": "Cannot compare a wallet against itself"}), 400

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
    # 1v1 mode: just the chosen opponent.
    # Default: ALL other wallets (full data, not restricted to shared collections).
    # Collection mode: wallets that traded that specific collection.
    with db.get_conn() as conn:
        if vs_addr:
            basket_rows = [{"wallet_address": vs_addr}]
        elif collection_filter:
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

    basket_has_data = False

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
            basket_has_data = True
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

                # Per-collection table: only accumulate for target wallet's
                # collections — except in 1v1, where opponent-only collections
                # are shown too (union of both wallets)
                if not vs_addr and col_addr not in target_collections:
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

    if vs_addr and not basket_has_data:
        msg = "Opponent has no trades in this time range." if since else "No trades found for opponent wallet."
        return jsonify({"error": msg}), 404

    # Build per-collection comparison list (1v1: union of both wallets' collections)
    collections_out = []
    for col_addr in (target_collections | set(basket_per_col)):
        tc = target_stats["per_collection"].get(col_addr) or {}
        bc = basket_per_col.get(col_addr, {})

        if tc:
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
        else:
            wallet_col = {
                "buys": 0, "sells": 0, "buy_eth": 0, "roi": None,
                "avg_holding_secs": None, "realized_pnl": 0,
                "wins": 0, "losses": 0, "win_rate": None, "matched_trades": 0,
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
        vs_row = db.get_wallet(conn, vs_addr) if vs_addr else None

    return jsonify({
        "wallet": {
            "address": wallet_addr,
            "name": wallet_row["name"] if wallet_row else None,
        },
        "mode": "1v1" if vs_addr else "basket",
        "vs_wallet": {
            "address": vs_addr,
            "name": vs_row["name"] if vs_row else None,
        } if vs_addr else None,
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
    with db.get_conn() as conn:
        sync_times = [db.get_sync_state(conn, w) for w in wallets]
    synced_at = min((s["last_synced_at"] for s in sync_times if s and s["last_synced_at"]), default=None)
    return jsonify({"buckets": buckets, "bucket_type": "daily", "total_pnl_eth": total_pnl,
                    "synced_at": synced_at})


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


# ── API: Market watchlist ──────────────────────────────────────────────────────

@app.route("/api/market")
def api_market():
    days = request.args.get("days", 1, type=float)
    db.init_db()
    now = int(_time.time())
    cutoff = int(now - days * 86400) if days > 0 else 0
    rows = []
    with db.get_conn() as conn:
        watchlist = db.list_watchlist(conn)
        trade_rows = conn.execute(
            "SELECT slug, COUNT(*) AS trades, SUM(eth_amount) AS volume "
            "FROM market_trades WHERE (:c=0 OR block_timestamp>=:c) GROUP BY slug",
            {"c": cutoff},
        ).fetchall()
        trade_map = {r["slug"]: dict(r) for r in trade_rows}
        # Oldest synced event per slug (unfiltered) so the UI can flag windows
        # reaching past the synced backfill
        oldest_map = {r["slug"]: r["oldest_ts"] for r in conn.execute(
            "SELECT slug, MIN(block_timestamp) AS oldest_ts FROM market_trades GROUP BY slug"
        ).fetchall()}
        # Seeded watchlist rows lack a contract address; fall back to the
        # collections table so the UI can link to the collection detail view
        col_addr_map = {r["slug"]: r["contract_address"] for r in conn.execute(
            "SELECT slug, contract_address FROM collections WHERE slug IS NOT NULL AND slug != ''"
        ).fetchall()}
        for w in watchlist:
            slug = w["slug"]
            snap = db.get_latest_floor_snapshot(conn, slug)
            old_snap = db.get_floor_at(conn, slug, cutoff) if cutoff else None
            ss = db.get_market_sync_state(conn, slug)
            tm = trade_map.get(slug, {})
            floor_eth = snap["floor_eth"] if snap else None
            offer_eth = snap["best_offer_eth"] if snap else None
            old_floor = old_snap["floor_eth"] if old_snap else None
            fp_change = (floor_eth - old_floor) / old_floor * 100 if (floor_eth is not None and old_floor and old_floor != 0) else None
            rows.append({
                "slug": slug,
                "name": w["name"],
                "contract_address": w["contract_address"] or col_addr_map.get(slug) or "",
                "floor_eth": floor_eth,
                "best_offer_eth": offer_eth,
                "trades": tm.get("trades", 0),
                "volume_eth": round(tm.get("volume") or 0, 4),
                "fp_change_pct": round(fp_change, 2) if fp_change is not None else None,
                "fp_change_since": old_snap["ts"] if fp_change is not None else None,
                "last_synced_at": ss["last_synced_at"] if ss else None,
                "oldest_event_ts": oldest_map.get(slug),
            })
    return jsonify({"rows": rows, "days": days})


@app.route("/api/market/sync", methods=["POST"])
def api_market_sync():
    data = request.get_json() or {}
    force = bool(data.get("force"))
    db.init_db()

    def generate():
        now = int(_time.time())
        with db.get_conn() as conn:
            watchlist = db.list_watchlist(conn)
        for w in watchlist:
            slug = w["slug"]
            try:
                with db.get_conn() as conn:
                    ss = db.get_market_sync_state(conn, slug)
                if not force and ss and ss["last_synced_at"] and (now - ss["last_synced_at"]) < MARKET_SYNC_MIN_INTERVAL:
                    yield f"data: {json.dumps({'type':'log','message':f'{slug}: skipped (fresh)'})}\n\n"
                    continue
                if not w["contract_address"]:
                    try:
                        info = fetch.fetch_collection_info(slug, retries=1)
                        if info.get("contract_address"):
                            with db.get_conn() as conn:
                                db.add_watchlist(conn, slug, info.get("name") or w["name"],
                                                 info["contract_address"])
                    except Exception:
                        pass
                yield f"data: {json.dumps({'type':'log','message':f'{slug}: fetching floor + bid...'})}\n\n"
                fp = fetch.fetch_floor_price(slug)
                _time.sleep(0.25)
                bo = fetch.fetch_best_offer(slug)
                _time.sleep(0.25)
                with db.get_conn() as conn:
                    db.insert_floor_snapshot(conn, slug, fp, bo, now)
                    if fp is not None or bo is not None:
                        db.upsert_collection_floor(conn, slug, fp, bo, now)
                after = max(
                    (ss["last_event_ts"] if ss and ss["last_event_ts"] else 0),
                    now - MARKET_BACKFILL_DAYS * 86400,
                )
                inserted = 0
                max_ts = ss["last_event_ts"] if ss and ss["last_event_ts"] else 0
                for page, events, truncated in fetch.iter_collection_sales(slug, after=after):
                    if page > 1:
                        yield f"data: {json.dumps({'type':'log','message':f'{slug}: page {page} ({len(events)} sales)'})}\n\n"
                    with db.get_conn() as conn:
                        for ev in events:
                            if db.insert_market_trade(conn, ev):
                                inserted += 1
                    if events:
                        max_ts = max(max_ts, max(e["block_timestamp"] for e in events))
                    if truncated:
                        yield f"data: {json.dumps({'type':'log','message':f'{slug}: WARNING page cap hit, older events in this window were skipped'})}\n\n"
                with db.get_conn() as conn:
                    db.set_market_sync_state(conn, slug, max_ts, now)
                yield f"data: {json.dumps({'type':'log','message':f'{slug}: {inserted} new trades, floor={fp} bid={bo}'})}\n\n"
            except Exception as exc:
                yield f"data: {json.dumps({'type':'log','message':f'{slug}: ERROR {exc}'})}\n\n"
        yield f"data: {json.dumps({'type':'done'})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@app.route("/api/market/trades/<slug>")
def api_market_trades(slug):
    days = request.args.get("days", 1, type=float)
    db.init_db()
    now = int(_time.time())
    cutoff = int(now - days * 86400) if days > 0 else 0
    with db.get_conn() as conn:
        q = ("SELECT block_timestamp, eth_amount, buyer_address, seller_address, tx_hash, nft_id, sell_type "
             "FROM market_trades WHERE slug=?")
        params = [slug]
        if cutoff:
            q += " AND block_timestamp>=?"
            params.append(cutoff)
        q += " ORDER BY block_timestamp DESC"
        rows = conn.execute(q, params).fetchall()
    return jsonify({"trades": [dict(r) for r in rows]})


@app.route("/api/watchlist/search")
def api_watchlist_search():
    import re as _re
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"results": []})
    db.init_db()
    like = f"%{q}%"
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT slug, name FROM collections "
            "WHERE slug IS NOT NULL AND slug != '' AND (name LIKE ? OR slug LIKE ?) "
            "ORDER BY name LIMIT 8",
            (like, like),
        ).fetchall()
        watched = {r["slug"] for r in db.list_watchlist(conn)}
    results = [{"slug": r["slug"], "name": r["name"] or r["slug"], "source": "local"}
               for r in rows if r["slug"] not in watched]
    # Also try the query as an OpenSea slug directly, so collections no tracked
    # wallet ever traded are still findable
    slug_guess = _re.sub(r"[^a-z0-9-]", "", q.lower().replace(" ", "-"))
    if slug_guess and slug_guess not in watched and all(r["slug"] != slug_guess for r in results):
        try:
            info = fetch.fetch_collection_info(slug_guess, retries=1)
            if info.get("name"):
                results.insert(0, {"slug": slug_guess, "name": info["name"], "source": "opensea"})
        except Exception:
            pass
    return jsonify({"results": results[:8]})


@app.route("/api/watchlist", methods=["POST"])
def api_watchlist_add():
    data = request.get_json() or {}
    slug = (data.get("slug") or "").strip().lower()
    if not slug:
        return jsonify({"error": "slug is required"}), 400
    try:
        info = fetch.fetch_collection_info(slug)
    except Exception:
        return jsonify({"error": "Collection not found on OpenSea"}), 400
    if not info.get("name"):
        return jsonify({"error": "Collection not found on OpenSea"}), 400
    db.init_db()
    with db.get_conn() as conn:
        db.add_watchlist(conn, slug, info["name"], info.get("contract_address", ""))
    return jsonify({"slug": slug, "name": info["name"]}), 201


@app.route("/api/watchlist/<slug>", methods=["DELETE"])
def api_watchlist_remove(slug):
    db.init_db()
    with db.get_conn() as conn:
        db.remove_watchlist(conn, slug.lower())
    return jsonify({"ok": True})


# ── API: Col Trading (sister project's market-wide sales data) ────────────────

@app.route("/api/coldata/collections")
def api_coldata_collections():
    db.init_db()
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT slug, COALESCE(name, slug) AS name, floor_price_eth, "
            "best_offer_eth, total_fee_bps, "
            "avg_gross_spread_eth, avg_net_spread_eth, "
            "avg_gross_spread_pct, avg_net_spread_pct, spread_pair_count, "
            "avg_daily_sales_alltime, avg_daily_sales_30d, total_trades "
            "FROM collections "
            "WHERE avg_net_spread_pct IS NOT NULL "
            "   OR slug IN (SELECT collection_slug FROM collection_sync_state) "
            "ORDER BY name"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/coldata/chart")
def api_coldata_chart():
    slugs = [s.strip() for s in request.args.get("collections", "").split(",") if s.strip()]
    if not slugs:
        return jsonify({})
    db.init_db()
    result = {}
    with db.get_conn() as conn:
        for slug in slugs:
            rows = conn.execute(
                "SELECT timestamp, price_eth, sale_type FROM sales "
                "WHERE collection_slug = ? ORDER BY timestamp", (slug,)
            ).fetchall()
            if not rows:
                continue
            daily = defaultdict(lambda: {"bids": 0, "listings": 0, "total_price": 0.0, "sale_count": 0})
            for row in rows:
                day = datetime.fromtimestamp(row["timestamp"], tz=_tz.utc).strftime("%Y-%m-%d")
                daily[day]["total_price"] += row["price_eth"]
                daily[day]["sale_count"]  += 1
                if row["sale_type"] == "bid":
                    daily[day]["bids"] += 1
                else:
                    daily[day]["listings"] += 1
            start = datetime.fromtimestamp(rows[0]["timestamp"],  tz=_tz.utc).date()
            end   = datetime.fromtimestamp(rows[-1]["timestamp"], tz=_tz.utc).date()
            all_days = []
            d = start
            while d <= end:
                all_days.append(d.isoformat())
                d += timedelta(days=1)
            result[slug] = {
                "days":        all_days,
                "bids":        [daily[d]["bids"]         for d in all_days],
                "listings":    [daily[d]["listings"]     for d in all_days],
                "total_price": [round(daily[d]["total_price"], 4) for d in all_days],
                "sale_count":  [daily[d]["sale_count"]   for d in all_days],
            }
    return jsonify(result)


if __name__ == "__main__":
    db.init_db()
    app.run(debug=True, port=5000, threaded=True, use_reloader=False)
