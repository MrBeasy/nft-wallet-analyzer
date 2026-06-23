"""Fetch NFT trade data from OpenSea v2 and Etherscan APIs."""

import os
import time
import logging
from typing import Optional

import requests

log = logging.getLogger(__name__)

OPENSEA_BASE = "https://api.opensea.io/api/v2"
ETHERSCAN_BASE = "https://api.etherscan.io/api"

# OpenSea's fee recipient on Ethereum mainnet
OPENSEA_FEE_RECIPIENT = "0x0000a26b00c1f0df003000390027140000faa719"

# ETH / WETH token addresses (treat both as ETH-denominated)
ETH_TOKENS = {
    "0x0000000000000000000000000000000000000000",  # native ETH
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
}


def _opensea_headers() -> dict:
    key = os.environ.get("OPENSEA_API_KEY", "")
    return {"accept": "application/json", "x-api-key": key}


def _etherscan_key() -> str:
    return os.environ.get("ETHERSCAN_API_KEY", "")


def _get(url: str, params: dict, headers: dict = None, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning("Rate limited, waiting %ds", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            log.warning("Request failed (%s), retrying...", e)
            time.sleep(1)
    return {}


# ── OpenSea ──────────────────────────────────────────────────────────────────

def fetch_collection_info(slug: str) -> dict:
    """
    Returns dict with keys: creator_fee_bps, opensea_fee_bps, name, contract_address.
    """
    url = f"{OPENSEA_BASE}/collections/{slug}"
    data = _get(url, {}, _opensea_headers())

    creator_fee_bps = 0
    opensea_fee_bps = 0
    name = data.get("name", slug)

    for fee in data.get("fees", []):
        recipient = (fee.get("recipient") or "").lower()
        bps = int(round(float(fee.get("fee", 0)) * 100))  # percent → bps
        required = fee.get("required", True)
        if recipient == OPENSEA_FEE_RECIPIENT:
            opensea_fee_bps += bps
        elif required:
            creator_fee_bps += bps

    # Primary contract address
    contract_address = ""
    contracts = data.get("contracts", [])
    for c in contracts:
        if c.get("chain", "").lower() == "ethereum":
            contract_address = c.get("address", "").lower()
            break

    return {
        "name": name,
        "contract_address": contract_address,
        "creator_fee_bps": creator_fee_bps,
        "opensea_fee_bps": opensea_fee_bps,
    }


def fetch_floor_price(slug: str) -> float | None:
    """Returns current floor price in ETH for a collection slug, or None if unavailable."""
    url = f"{OPENSEA_BASE}/collections/{slug}/stats"
    data = _get(url, {}, _opensea_headers())
    total = data.get("total") or {}
    floor = total.get("floor_price")
    return float(floor) if floor is not None else None


def fetch_best_offer(slug: str) -> float | None:
    """Returns best collection offer in ETH, or None if no offers exist."""
    url = f"{OPENSEA_BASE}/offers/collection/{slug}"
    data = _get(url, {}, _opensea_headers())
    offers = data.get("offers") or []
    if not offers:
        return None
    try:
        return int(offers[0]["price"]["current"]["value"]) / 1e18
    except (KeyError, TypeError, ValueError):
        return None


def fetch_sale_events(wallet_address: str, cursor: Optional[str] = None) -> tuple[list, Optional[str]]:
    """
    Fetch one page of sale events for a wallet from OpenSea.
    Returns (events_list, next_cursor).
    Only returns ETH/WETH-denominated trades.
    """
    url = f"{OPENSEA_BASE}/events/accounts/{wallet_address}"
    params = {
        "event_type": "sale",
        "chain": "ethereum",
        "limit": 50,
    }
    if cursor:
        params["next"] = cursor

    data = _get(url, params, _opensea_headers())
    raw_events = data.get("asset_events", [])
    next_cursor = data.get("next")

    events = []
    for ev in raw_events:
        payment = ev.get("payment") or {}

        # OpenSea v2 returns flat payment fields: token_address, symbol, decimals
        # (not nested under a "token" sub-object)
        token_addr = (payment.get("token_address") or "").lower()
        symbol = (payment.get("symbol") or "").upper()

        # Skip non-ETH payments
        if token_addr not in ETH_TOKENS and symbol not in ("ETH", "WETH"):
            continue

        quantity_str = payment.get("quantity", "0")
        try:
            eth_amount = int(quantity_str) / 1e18
        except (ValueError, TypeError):
            eth_amount = 0.0

        if eth_amount <= 0:
            continue

        nft = ev.get("nft") or {}

        # OpenSea v2 returns seller/buyer as plain address strings, not objects
        seller_raw = ev.get("seller") or ""
        buyer_raw = ev.get("buyer") or ""
        seller = (seller_raw.get("address", "") if isinstance(seller_raw, dict) else str(seller_raw)).lower()
        buyer = (buyer_raw.get("address", "") if isinstance(buyer_raw, dict) else str(buyer_raw)).lower()
        wallet = wallet_address.lower()

        if wallet not in (seller, buyer):
            continue  # shouldn't happen, but guard

        side = "sell" if seller == wallet else "buy"

        # Determine sell_type from order maker:
        # maker == seller → seller listed it (listing sale)
        # maker == buyer  → buyer placed an offer that seller accepted (bid dump)
        sell_type = None
        if side == "sell":
            maker_raw = ev.get("maker") or ""
            maker = (maker_raw.get("address", "") if isinstance(maker_raw, dict)
                     else str(maker_raw)).lower()
            if maker == seller:
                sell_type = "listing"
            elif maker == buyer:
                sell_type = "bid"

        events.append({
            "tx_hash": (ev.get("transaction") or "").lower(),
            "block_timestamp": ev.get("closing_date") or 0,
            "side": side,
            "eth_amount": eth_amount,
            "buyer_address": buyer,
            "seller_address": seller,
            "collection_slug": nft.get("collection", ""),
            "collection_address": (nft.get("contract") or "").lower(),
            "nft_id": str(nft.get("identifier", "")),
            "marketplace": "opensea",
            "sell_type": sell_type,
        })

    return events, next_cursor


def fetch_all_sale_events(wallet_address: str, from_cursor: Optional[str] = None,
                          progress_cb=None) -> tuple[list, Optional[str]]:
    """
    Paginate through all sale events. Returns (all_events, last_cursor_used).
    Stops when no more pages or tx_hash already seen (incremental sync).
    """
    all_events = []
    cursor = from_cursor
    page = 0

    while True:
        events, next_cursor = fetch_sale_events(wallet_address, cursor)
        page += 1

        if progress_cb:
            progress_cb(page, len(events))

        all_events.extend(events)

        if not next_cursor or not events:
            break

        cursor = next_cursor
        time.sleep(0.3)  # be polite to the API

    return all_events, cursor


# ── Etherscan ─────────────────────────────────────────────────────────────────

def fetch_tx_gas(tx_hash: str) -> float:
    """
    Returns gas cost in ETH for a transaction hash.
    Uses eth_getTransactionReceipt + eth_getTransactionByHash.
    """
    key = _etherscan_key()

    # Get receipt for gasUsed
    receipt = _get(ETHERSCAN_BASE, {
        "module": "proxy",
        "action": "eth_getTransactionReceipt",
        "txhash": tx_hash,
        "apikey": key,
    })
    result = receipt.get("result") or {}
    gas_used = int(result.get("gasUsed", "0x0"), 16)

    if gas_used == 0:
        return 0.0

    # Get tx for gasPrice (or effectiveGasPrice from receipt)
    effective_gas_price = result.get("effectiveGasPrice")
    if effective_gas_price:
        gas_price_wei = int(effective_gas_price, 16)
    else:
        tx_data = _get(ETHERSCAN_BASE, {
            "module": "proxy",
            "action": "eth_getTransactionByHash",
            "txhash": tx_hash,
            "apikey": key,
        })
        tx_result = tx_data.get("result") or {}
        gas_price_wei = int(tx_result.get("gasPrice", "0x0"), 16)

    return (gas_used * gas_price_wei) / 1e18


def fetch_gas_for_trades(trades: list, progress_cb=None) -> dict:
    """
    Given a list of trade dicts with tx_hash, returns {tx_hash: gas_eth}.
    Batches unique hashes to avoid redundant calls.
    """
    unique_hashes = list({t["tx_hash"] for t in trades if t.get("tx_hash")})
    gas_map = {}

    for i, tx_hash in enumerate(unique_hashes):
        if progress_cb:
            progress_cb(i + 1, len(unique_hashes))
        try:
            gas_map[tx_hash] = fetch_tx_gas(tx_hash)
        except Exception as e:
            log.warning("Could not fetch gas for %s: %s", tx_hash, e)
            gas_map[tx_hash] = 0.0
        time.sleep(0.21)  # Etherscan free tier: ~5 req/s

    return gas_map
