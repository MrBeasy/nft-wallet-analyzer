"""PnL and trade analytics for a wallet's NFT history."""

from collections import defaultdict
from datetime import datetime, timezone


def _eth(val: float) -> str:
    return f"{val:+.4f} ETH" if val != 0 else " 0.0000 ETH"


def _ts(unix: int) -> str:
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%d")


def _days(seconds: float) -> str:
    d = seconds / 86400
    if d < 1:
        return f"{seconds/3600:.1f}h"
    return f"{d:.1f}d"


# ── FIFO matching ─────────────────────────────────────────────────────────────

def _match_fifo(buys: list, sells: list) -> tuple[list, list]:
    """
    Match buys to sells using FIFO (both lists must be sorted by timestamp).
    Returns (matched_round_trips, unmatched_sells).

    Unmatched sells are sells with no prior buy — e.g. airdropped / minted NFTs
    that were sold without a tracked purchase.
    """
    buy_queue = sorted(buys, key=lambda t: t["block_timestamp"])
    sell_queue = sorted(sells, key=lambda t: t["block_timestamp"])

    matched = []
    unmatched_sells = []
    buy_idx = 0

    for sell in sell_queue:
        # Earliest available buy that happened before (or at) this sell
        if buy_idx < len(buy_queue) and buy_queue[buy_idx]["block_timestamp"] <= sell["block_timestamp"]:
            buy = buy_queue[buy_idx]
            buy_idx += 1
            holding = sell["block_timestamp"] - buy["block_timestamp"]
            v = sell.get("total_fee_bps")
            sell_fee_bps = 100 if v is None else v  # 1% fallback when collection not yet in DB
            sell_fees_eth = sell["eth_amount"] * sell_fee_bps / 10000
            sell_net = sell["eth_amount"] - sell_fees_eth
            pnl = sell_net - buy["eth_amount"] - buy.get("gas_eth", 0) - sell.get("gas_eth", 0)
            matched.append({
                "buy_ts": buy["block_timestamp"],
                "sell_ts": sell["block_timestamp"],
                "buy_eth": buy["eth_amount"],
                "sell_eth": sell["eth_amount"],
                "sell_fees_eth": sell_fees_eth,
                "sell_fee_bps": sell_fee_bps,
                "sell_net_eth": sell_net,
                "buy_gas": buy.get("gas_eth", 0),
                "sell_gas": sell.get("gas_eth", 0),
                "holding_secs": holding,
                "pnl_eth": pnl,
                "nft_id": sell["nft_id"],
                "collection_address": sell["collection_address"],
                "collection_slug": sell.get("collection_slug", ""),
                "collection_name": sell.get("collection_name") or sell.get("collection_slug", ""),
                "sell_type": sell.get("sell_type"),
            })
        else:
            # No buy found before this sell — airdrop/transfer/data gap
            unmatched_sells.append(sell)

    return matched, unmatched_sells


# ── Main analytics ─────────────────────────────────────────────────────────────

def compute_analytics(trades: list) -> dict:
    """
    trades: list of sqlite3.Row (or dicts) from db.get_trades().
    Returns a rich analytics dict.
    """
    if not trades:
        return {}

    # Convert sqlite3.Row to dict once up front; every loop below indexes freely.
    trades = [dict(t) for t in trades]

    # Separate buys and sells
    buys_by_nft = defaultdict(list)   # key: (collection_address, nft_id)
    sells_by_nft = defaultdict(list)

    open_positions = defaultdict(list)  # buys with no matching sell yet
    total_buy_eth = 0.0
    total_sell_eth = 0.0
    total_gas_eth = 0.0
    total_buys_count = 0
    total_sells_count = 0

    for row in trades:
        key = (row["collection_address"], row["nft_id"])
        if row["side"] == "buy":
            buys_by_nft[key].append(row)
            total_buy_eth += row["eth_amount"]
            total_buys_count += 1
        else:
            sells_by_nft[key].append(row)
            total_sell_eth += row["eth_amount"]
            total_sells_count += 1
        total_gas_eth += row.get("gas_eth", 0) or 0

    # FIFO match per NFT
    all_matched = []
    all_unmatched_sells = []
    for key in set(list(buys_by_nft.keys()) + list(sells_by_nft.keys())):
        buys = buys_by_nft.get(key, [])
        sells = sells_by_nft.get(key, [])
        matched, unmatched_sells = _match_fifo(buys, sells)
        all_matched.extend(matched)
        all_unmatched_sells.extend(unmatched_sells)

        # Unmatched buys = open positions
        matched_buy_count = len(matched)
        unmatched_buys = sorted(buys, key=lambda t: t["block_timestamp"])[matched_buy_count:]
        open_positions[key].extend(unmatched_buys)

    # Subtract unmatched sells from totals (NFTs sold with no tracked buy)
    unmatched_sell_eth = sum(u["eth_amount"] for u in all_unmatched_sells)
    total_sell_eth -= unmatched_sell_eth
    # Fees on unmatched sells (approximate — using the sell row's total_fee_bps)
    unmatched_fees_eth = sum(
        u["eth_amount"] * (100 if (v := u.get("total_fee_bps")) is None else v) / 10000
        for u in all_unmatched_sells
    )

    # Realized PnL and fees
    realized_pnl = sum(m["pnl_eth"] for m in all_matched)
    total_fees_eth = sum(m["sell_fees_eth"] for m in all_matched)
    winning_trades = [m for m in all_matched if m["pnl_eth"] > 0]
    losing_trades = [m for m in all_matched if m["pnl_eth"] <= 0]

    # Holding times
    holding_times = [m["holding_secs"] for m in all_matched if m["holding_secs"] >= 0]
    avg_holding_secs = sum(holding_times) / len(holding_times) if holding_times else 0

    # Per-collection stats
    col_stats = defaultdict(lambda: {
        "name": "",
        "buys": 0,
        "sells": 0,
        "buy_eth": 0.0,
        "sell_eth": 0.0,
        "fees_eth": 0.0,
        "creator_fee_bps": 0,
        "opensea_fee_bps": 0,
        "total_fee_bps": 0,
        "realized_pnl": 0.0,
        "matched_trades": 0,
        "holding_times": [],
        "open_positions": 0,
        "wins": 0,
        "losses": 0,
        "last_trade_ts": 0,
        "first_trade_ts": None,
    })

    unmatched_sell_keys = {(u["collection_address"], u["nft_id"]) for u in all_unmatched_sells}

    for row in trades:
        col = row["collection_address"]
        col_stats[col]["name"] = row.get("collection_name") or row.get("collection_slug", col[:10])
        v = row.get("total_fee_bps")
        col_stats[col]["total_fee_bps"] = 100 if v is None else v
        ts = row.get("block_timestamp") or 0
        if ts > col_stats[col]["last_trade_ts"]:
            col_stats[col]["last_trade_ts"] = ts
        if ts and (col_stats[col]["first_trade_ts"] is None or ts < col_stats[col]["first_trade_ts"]):
            col_stats[col]["first_trade_ts"] = ts
        if row["side"] == "buy":
            col_stats[col]["buys"] += 1
            col_stats[col]["buy_eth"] += row["eth_amount"]
        else:
            # Exclude sells that have no corresponding buy from collection stats
            key = (row["collection_address"], row["nft_id"])
            if key not in unmatched_sell_keys:
                col_stats[col]["sells"] += 1
                col_stats[col]["sell_eth"] += row["eth_amount"]

    for m in all_matched:
        col = m["collection_address"]
        col_stats[col]["realized_pnl"] += m["pnl_eth"]
        col_stats[col]["fees_eth"] += m["sell_fees_eth"]
        col_stats[col]["matched_trades"] += 1
        if m["holding_secs"] >= 0:
            col_stats[col]["holding_times"].append(m["holding_secs"])
        if m["pnl_eth"] > 0:
            col_stats[col]["wins"] += 1
        else:
            col_stats[col]["losses"] += 1

    for key, pos_list in open_positions.items():
        col = key[0]
        col_stats[col]["open_positions"] += len(pos_list)

    # Finalize per-collection avg holding, ROI, and win rate
    for col, s in col_stats.items():
        ht = s["holding_times"]
        s["avg_holding_secs"] = sum(ht) / len(ht) if ht else None
        s["roi"] = s["realized_pnl"] / s["buy_eth"] if s["buy_eth"] else 0
        total_matched = s["wins"] + s["losses"]
        s["win_rate"] = s["wins"] / total_matched if total_matched else 0

    # Open positions value (cost basis)
    open_cost_basis = sum(
        b["eth_amount"] + b.get("gas_eth", 0)
        for positions in open_positions.values()
        for b in positions
    )
    open_count = sum(len(p) for p in open_positions.values())

    return {
        "summary": {
            "total_trades": len(trades),
            "total_buys": total_buys_count,
            "total_sells": total_sells_count,
            "total_buy_eth": total_buy_eth,
            "total_sell_eth": total_sell_eth,
            "total_gas_eth": total_gas_eth,
            "total_fees_eth": total_fees_eth,
            "realized_pnl_eth": realized_pnl,
            "unmatched_sells": len(all_unmatched_sells),
            "unmatched_sell_eth": unmatched_sell_eth,
            "unmatched_fees_eth": unmatched_fees_eth,
            "open_positions": open_count,
            "open_cost_basis_eth": open_cost_basis,
            "win_rate": len(winning_trades) / len(all_matched) if all_matched else 0,
            "roi": realized_pnl / total_buy_eth if total_buy_eth else 0,
            "avg_win_eth": sum(m["pnl_eth"] for m in winning_trades) / len(winning_trades) if winning_trades else 0,
            "avg_loss_eth": sum(m["pnl_eth"] for m in losing_trades) / len(losing_trades) if losing_trades else 0,
            "avg_holding": avg_holding_secs,
            "collections_traded": len(col_stats),
        },
        "per_collection": dict(col_stats),
        "matched_trades": all_matched,
        "open_positions": {
            f"{k[0]}:{k[1]}": [dict(b) for b in v]
            for k, v in open_positions.items() if v
        },
    }


def recompute_for_window(full_result: dict, since_ts: int, window_trades: list) -> dict:
    """
    Derives a time-windowed analytics result from a full-history result.
    - Matched trades: filtered to sell_ts >= since_ts (preserving correct FIFO cost basis)
    - Buy/sell totals and open positions: from window_trades (trades within the window)
    """
    window_trades = [dict(t) for t in window_trades]

    matched = [m for m in full_result.get("matched_trades", []) if m["sell_ts"] >= since_ts]
    matched_nft_ids = {(m["collection_address"], m["nft_id"]) for m in matched}

    window_buys  = [t for t in window_trades if t["side"] == "buy"]
    window_sells = [t for t in window_trades if t["side"] == "sell"]
    total_buy_eth     = sum(t["eth_amount"] for t in window_buys)
    total_gas_eth     = sum(t.get("gas_eth", 0) or 0 for t in window_trades)
    total_buys_count  = len(window_buys)
    total_sells_count = len(window_sells)

    unmatched_sells = [t for t in window_sells
                       if (t["collection_address"], t["nft_id"]) not in matched_nft_ids]
    unmatched_sell_eth  = sum(u["eth_amount"] for u in unmatched_sells)
    unmatched_fees_eth  = sum(
        u["eth_amount"] * (100 if (v := u.get("total_fee_bps")) is None else v) / 10000
        for u in unmatched_sells
    )
    total_sell_eth = sum(m["sell_eth"] for m in matched)

    realized_pnl   = sum(m["pnl_eth"]      for m in matched)
    total_fees_eth = sum(m["sell_fees_eth"] for m in matched)
    wins   = [m for m in matched if m["pnl_eth"] > 0]
    losses = [m for m in matched if m["pnl_eth"] <= 0]
    holding_times = [m["holding_secs"] for m in matched if m["holding_secs"] >= 0]
    avg_holding   = sum(holding_times) / len(holding_times) if holding_times else 0

    open_positions = {}
    for key_str, pos_list in full_result.get("open_positions", {}).items():
        in_window = [b for b in pos_list if b.get("block_timestamp", 0) >= since_ts]
        if in_window:
            open_positions[key_str] = in_window
    open_count      = sum(len(p) for p in open_positions.values())
    open_cost_basis = sum(
        b["eth_amount"] + b.get("gas_eth", 0)
        for pos in open_positions.values() for b in pos
    )

    unmatched_sell_keys = {(u["collection_address"], u["nft_id"]) for u in unmatched_sells}
    col_stats = defaultdict(lambda: {
        "name": "", "buys": 0, "sells": 0, "buy_eth": 0.0, "sell_eth": 0.0,
        "fees_eth": 0.0, "creator_fee_bps": 0, "opensea_fee_bps": 0,
        "total_fee_bps": 0, "realized_pnl": 0.0, "matched_trades": 0,
        "holding_times": [], "open_positions": 0, "wins": 0, "losses": 0,
        "last_trade_ts": 0, "first_trade_ts": None,
    })
    for row in window_trades:
        col = row["collection_address"]
        col_stats[col]["name"] = row.get("collection_name") or row.get("collection_slug", col[:10])
        v = row.get("total_fee_bps")
        col_stats[col]["total_fee_bps"] = 100 if v is None else v
        ts = row.get("block_timestamp") or 0
        if ts > col_stats[col]["last_trade_ts"]:
            col_stats[col]["last_trade_ts"] = ts
        if ts and (col_stats[col]["first_trade_ts"] is None or ts < col_stats[col]["first_trade_ts"]):
            col_stats[col]["first_trade_ts"] = ts
        if row["side"] == "buy":
            col_stats[col]["buys"]    += 1
            col_stats[col]["buy_eth"] += row["eth_amount"]
        elif (row["collection_address"], row["nft_id"]) not in unmatched_sell_keys:
            col_stats[col]["sells"]    += 1
            col_stats[col]["sell_eth"] += row["eth_amount"]
    for m in matched:
        col = m["collection_address"]
        col_stats[col]["realized_pnl"]   += m["pnl_eth"]
        col_stats[col]["fees_eth"]       += m["sell_fees_eth"]
        col_stats[col]["matched_trades"] += 1
        if m["holding_secs"] >= 0:
            col_stats[col]["holding_times"].append(m["holding_secs"])
        if m["pnl_eth"] > 0:
            col_stats[col]["wins"]   += 1
        else:
            col_stats[col]["losses"] += 1
    for key_str, pos_list in open_positions.items():
        col = key_str.split(":")[0]
        col_stats[col]["open_positions"] += len(pos_list)
    for col, s in col_stats.items():
        ht = s["holding_times"]
        s["avg_holding_secs"] = sum(ht) / len(ht) if ht else None
        s["roi"]      = s["realized_pnl"] / s["buy_eth"] if s["buy_eth"] else 0
        total_m = s["wins"] + s["losses"]
        s["win_rate"] = s["wins"] / total_m if total_m else 0

    return {
        "summary": {
            "total_trades":        len(window_trades),
            "total_buys":          total_buys_count,
            "total_sells":         total_sells_count,
            "total_buy_eth":       total_buy_eth,
            "total_sell_eth":      total_sell_eth,
            "total_gas_eth":       total_gas_eth,
            "total_fees_eth":      total_fees_eth,
            "realized_pnl_eth":    realized_pnl,
            "unmatched_sells":     len(unmatched_sells),
            "unmatched_sell_eth":  unmatched_sell_eth,
            "unmatched_fees_eth":  unmatched_fees_eth,
            "open_positions":      open_count,
            "open_cost_basis_eth": open_cost_basis,
            "win_rate":     len(wins) / len(matched) if matched else 0,
            "roi":          realized_pnl / total_buy_eth if total_buy_eth else 0,
            "avg_win_eth":  sum(m["pnl_eth"] for m in wins)   / len(wins)   if wins   else 0,
            "avg_loss_eth": sum(m["pnl_eth"] for m in losses) / len(losses) if losses else 0,
            "avg_holding":  avg_holding,
            "collections_traded": len(col_stats),
        },
        "per_collection": dict(col_stats),
        "matched_trades": matched,
        "open_positions": open_positions,
    }


# ── Player Card ───────────────────────────────────────────────────────────────

def _diamond_score(avg_secs, flipper_days=7, trader_days=180):
    if not avg_secs:
        return {"label": "No Data"}
    d = avg_secs / 86400
    if d < flipper_days:  return {"label": "Flipper"}
    if d < trader_days:   return {"label": "Trader"}
    return                       {"label": "Diamond Hand"}


def compute_player_card(trades, per_collection, summary, floor_data, open_positions=None):
    import time as _time

    timestamps = sorted(int(t["block_timestamp"]) for t in trades)
    now = _time.time()

    # Wallet age
    first_ts = timestamps[0] if timestamps else None
    wallet_age_days = (now - first_ts) / 86400 if first_ts else None

    # Trades last 30d
    cutoff_30d = now - 30 * 86400
    trades_last_30d = sum(1 for ts in timestamps if ts >= cutoff_30d)

    # Bot score
    total = len(timestamps)
    if total <= 1:
        rapid_pairs, bot_ratio = 0, 0.0
    else:
        rapid_pairs = sum(
            1 for i in range(len(timestamps) - 1)
            if timestamps[i + 1] - timestamps[i] <= 60
        )
        bot_ratio = rapid_pairs / (total - 1)
    if bot_ratio < 0.1:
        bot_label = "Clean"
    elif bot_ratio < 0.3:
        bot_label = "Suspicious"
    else:
        bot_label = "Bot-like"

    # Diamond hands — label is the backend default; frontend overrides with user thresholds
    dh = _diamond_score(summary.get("avg_holding", 0))
    dh["avg_holding_secs"] = summary.get("avg_holding") or None

    # Collector style — based on simultaneous holding behaviour
    buy_ts_list  = sorted(int(t["block_timestamp"]) for t in trades if t["side"] == "buy")
    sell_ts_list = sorted(int(t["block_timestamp"]) for t in trades if t["side"] == "sell")

    # Build a merged timeline; at equal timestamps sell first (release before acquire)
    timeline = [(ts, 1) for ts in buy_ts_list] + [(ts, -1) for ts in sell_ts_list]
    timeline.sort(key=lambda e: (e[0], e[1]))

    holdings = 0
    max_concurrent = 0
    holdings_at_buy = []
    for ts, delta in timeline:
        if delta == 1:
            holdings_at_buy.append(holdings)
            holdings += 1
        else:
            holdings = max(0, holdings - 1)
        if holdings > max_concurrent:
            max_concurrent = holdings

    avg_concurrent = sum(holdings_at_buy) / len(holdings_at_buy) if holdings_at_buy else 0.0

    # Batch buys: buy events within 5 minutes of the next buy
    BATCH_WINDOW = 300
    batch_buys = sum(
        1 for i in range(len(buy_ts_list) - 1)
        if buy_ts_list[i + 1] - buy_ts_list[i] <= BATCH_WINDOW
    ) if len(buy_ts_list) > 1 else 0
    batch_sells = sum(
        1 for i in range(len(sell_ts_list) - 1)
        if sell_ts_list[i + 1] - sell_ts_list[i] <= BATCH_WINDOW
    ) if len(sell_ts_list) > 1 else 0

    total_buys_count = len(buy_ts_list)
    batch_ratio = batch_buys / total_buys_count if total_buys_count else 0.0

    if max_concurrent <= 1:
        style_label = "Serial"
    elif batch_ratio >= 0.2:
        style_label = "Bulk Trader"
    else:
        style_label = "Accumulator"

    # uPnL vs floor — sum over individual open positions
    upnl_eth = None
    positions_with_floor = 0
    total_open = sum(len(v) for v in open_positions.values()) if open_positions else 0
    if open_positions and floor_data:
        upnl_sum = 0.0
        for buys in open_positions.values():
            for b in buys:
                slug = b.get("collection_slug")
                floor_eth = (floor_data.get(slug) or {}).get("floor_price_eth")
                if not floor_eth or floor_eth <= 0:
                    continue
                v = b.get("total_fee_bps")
                fee_bps = 100 if v is None else v
                net = floor_eth * (1 - fee_bps / 10000)
                cost = b["eth_amount"] + (b.get("gas_eth") or 0)
                upnl_sum += net - cost
                positions_with_floor += 1
        if positions_with_floor:
            upnl_eth = upnl_sum

    return {
        "wallet_age_days": wallet_age_days,
        "first_trade_ts": first_ts,
        "trades_last_30d": trades_last_30d,
        "total_trades": total,
        "bot_score": {
            "score": bot_ratio,
            "label": bot_label,
            "rapid_pairs": rapid_pairs,
        },
        "diamond_hands": dh,
        "collector_style": {
            "label": style_label,
            "max_concurrent": max_concurrent,
            "avg_concurrent": round(avg_concurrent, 1),
            "batch_buys": batch_buys,
            "batch_sells": batch_sells,
        },
        "upnl_floor": {
            "upnl_eth": upnl_eth,
            "positions_with_floor": positions_with_floor,
            "total_open": total_open,
        },
    }


# ── Entity analytics (wallet clusters) ────────────────────────────────────────

def split_wash_trades(trades: list, members: set) -> tuple:
    """Split into (clean, wash) where wash rows have both buyer and seller in members."""
    clean, wash = [], []
    for t in trades:
        if t.get("buyer_address") in members and t.get("seller_address") in members:
            wash.append(t)
        else:
            clean.append(t)
    return clean, wash


def compute_entity_analytics(trades: list, members: set) -> dict:
    """FIFO analytics for a wallet cluster with wash trades surfaced as explicit cost."""
    clean, wash = split_wash_trades([dict(t) for t in trades], members)
    result = compute_analytics(clean)
    wash_gas  = sum(w.get("gas_eth") or 0 for w in wash)
    wash_fees = sum(w["eth_amount"] * (100 if (v := w.get("total_fee_bps")) is None else v) / 10000
                    for w in wash if w["side"] == "sell")
    wash_cost = wash_gas + wash_fees
    if not result:
        return {
            "summary": {
                "total_trades": 0, "total_buys": 0, "total_sells": 0,
                "total_buy_eth": 0.0, "total_sell_eth": 0.0,
                "total_gas_eth": wash_gas, "total_fees_eth": wash_fees,
                "realized_pnl_eth": -wash_cost,
                "unmatched_sells": 0, "unmatched_sell_eth": 0.0, "unmatched_fees_eth": 0.0,
                "open_positions": 0, "open_cost_basis_eth": 0.0,
                "win_rate": 0.0, "roi": 0.0,
                "avg_win_eth": 0.0, "avg_loss_eth": 0.0,
                "avg_holding": 0, "collections_traded": 0,
                "wash_legs": len(wash), "wash_cost_eth": wash_cost,
            },
            "per_collection": {}, "matched_trades": [], "open_positions": {},
        }
    s = result["summary"]
    s["wash_legs"]         = len(wash)
    s["wash_cost_eth"]     = wash_cost
    s["realized_pnl_eth"] -= wash_cost
    s["total_gas_eth"]    += wash_gas
    s["total_fees_eth"]   += wash_fees
    return result


# ── Formatters ────────────────────────────────────────────────────────────────

def format_summary(analytics: dict, wallet_name: str = None,
                   unrealized_pnl_eth: float = None, floor_value_eth: float = None) -> str:
    if not analytics:
        return "No trade data found."
    s = analytics["summary"]
    header = f"=== WALLET SUMMARY{' — ' + wallet_name if wallet_name else ''} ==="
    lines = [
        header,
        f"  Total trades      : {s['total_trades']}  ({s['total_buys']} buys / {s['total_sells']} sells)",
        f"  Collections       : {s['collections_traded']}",
        f"  Total spent (buy) : {s['total_buy_eth']:.4f} ETH",
        f"  Matched sell rcvd : {s['total_sell_eth']:.4f} ETH  ({s['total_sells'] - s['unmatched_sells']} matched sells)",
        f"  Royalties + fees  : {s['total_fees_eth']:.4f} ETH  (on matched sells)",
        f"  Gas costs         : {s['total_gas_eth']:.4f} ETH",
        f"  Realized PnL      : {_eth(s['realized_pnl_eth'])}",
        f"  Win rate          : {s['win_rate']*100:.1f}%  (avg win {_eth(s['avg_win_eth'])}  avg loss {_eth(s['avg_loss_eth'])})",
        f"  ROI               : {s['roi']*100:+.1f}%",
        f"  Avg holding time  : {_days(s['avg_holding']) if s['avg_holding'] else 'n/a'}",
        f"  Open positions    : {s['open_positions']}  (cost basis {s['open_cost_basis_eth']:.4f} ETH)",
        *([ f"  Unrealized PnL    : {_eth(unrealized_pnl_eth)}  (floor value {floor_value_eth:.4f} ETH)" ]
          if unrealized_pnl_eth is not None else []),
        *([ f"  Unmatched sells   : {s['unmatched_sells']} trades / {s['unmatched_sell_eth']:.4f} ETH gross"
            f"  — no buy record (minted/received); excluded from PnL" ]
          if s['unmatched_sells'] else []),
    ]
    return "\n".join(lines)


def format_collections(analytics: dict, top_n: int = 20) -> str:
    if not analytics:
        return ""
    from tabulate import tabulate
    rows = []
    for addr, s in analytics["per_collection"].items():
        avg_hold = _days(s["avg_holding_secs"]) if s.get("avg_holding_secs") else "-"
        fee_pct = f"{s['total_fee_bps']/100:.1f}%" if s.get("total_fee_bps") else "-"
        roi_pct = f"{s['roi']*100:+.1f}%" if s.get("buy_eth") else "-"
        rows.append([
            (s["name"] or addr[:10] + "...")[:24],
            s["buys"],
            s["sells"],
            f"{s['buy_eth']:.3f}",
            f"{s['sell_eth']:.3f}",
            f"{s['fees_eth']:.4f}",
            fee_pct,
            f"{s['realized_pnl']:+.4f}",
            roi_pct,
            avg_hold,
            s["open_positions"],
        ])
    rows.sort(key=lambda r: float(r[7]), reverse=True)
    headers = ["Collection", "Buys", "Sells", "Spent ETH", "Rcvd ETH", "Fees ETH", "Fee%", "rPnL", "ROI", "Avg Hold", "Open"]
    return tabulate(rows[:top_n], headers=headers, tablefmt="simple")


def format_open_positions(analytics: dict, floor_prices: dict = None) -> str:
    """
    floor_prices: optional {collection_slug: floor_price_eth}.
    When provided, adds Floor ETH and Est. PnL columns.
    Est. PnL = floor * (1 - fee_bps/10000) - buy_eth - buy_gas  (sell gas excluded).
    """
    if not analytics or not analytics.get("open_positions"):
        return "No open positions."
    from tabulate import tabulate
    rows = []
    total_cost = 0.0
    total_floor_net = 0.0
    has_any_floor = False

    for key, buys in analytics["open_positions"].items():
        for b in buys:
            col_name = (b.get("collection_name") or b.get("collection_slug") or key.split(":")[0][:10])[:20]
            slug = b.get("collection_slug")
            cost = b["eth_amount"] + (b.get("gas_eth") or 0)
            total_cost += cost

            row = [
                col_name,
                b["nft_id"],
                _ts(b["block_timestamp"]),
                f"{b['eth_amount']:.4f}",
            ]

            if floor_prices is not None:
                floor = floor_prices.get(slug) if slug else None
                if floor is not None:
                    has_any_floor = True
                    v = b.get("total_fee_bps")
                    fee_bps = 100 if v is None else v
                    floor_net = floor * (1 - fee_bps / 10000)
                    est_pnl = floor_net - cost
                    total_floor_net += floor_net
                    row.extend([f"{floor:.4f}", f"{est_pnl:+.4f}"])
                else:
                    row.extend(["n/a", "n/a"])

            rows.append(row)

    rows.sort(key=lambda r: r[2])

    headers = ["Collection", "NFT ID", "Bought", "Cost ETH"]
    if floor_prices is not None:
        headers.extend(["Floor ETH", "Est. PnL"])

    table = tabulate(rows, headers=headers, tablefmt="simple")

    if has_any_floor:
        est_total = total_floor_net - total_cost
        table += (
            f"\n\nCost basis: {total_cost:.4f} ETH  |  "
            f"Floor value (net fees): {total_floor_net:.4f} ETH  |  "
            f"Total est. PnL: {est_total:+.4f} ETH"
        )

    return table
