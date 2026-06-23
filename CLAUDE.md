# NFT Player Analysis

Flask + SQLite tool that syncs a wallet's NFT trade history from OpenSea and computes FIFO PnL, per-collection stats, and cross-wallet meta analysis.

## Stack
- Python 3.13, SQLite (`nft_trades.db`), Flask, requests, python-dotenv, tabulate
- No ORM — raw sqlite3 with `sqlite3.Row` (dict-like access)

## Files
| File | Role |
|---|---|
| `db.py` | Schema, `get_conn()`, all DB helpers |
| `fetch.py` | OpenSea v2 API (sale events) + Etherscan (gas) + floor prices |
| `analytics.py` | FIFO PnL matching, per-collection stats (wins/losses/holding times), `compute_analytics()` |
| `app.py` | Flask server — all API routes + SSE sync streaming |
| `main.py` | CLI wrapper (`sync`, `report`, `trades`, `wallet` subcommands) |
| `templates/index.html` | Single-page frontend (vanilla JS, hash router, sortable tables) |

## DB Schema (key tables)
- `trades` — one row per buy/sell event: `wallet_address, tx_hash, block_timestamp, side, eth_amount, gas_eth, collection_address, collection_slug, nft_id`
- `collections` — `contract_address PK, slug, name, creator_fee_bps, opensea_fee_bps, total_fee_bps`
- `sync_state` — `wallet_address PK, last_cursor, full_sync_complete`
- `wallets` — optional labels/notes per address
- `wallet_summaries` — cached analytics snapshot per wallet

## Key design decisions
- **FIFO matching**: buys and sells matched per `(collection_address, nft_id)`, oldest buy first
- **ETH/WETH only**: USDC and other token trades filtered out
- **Incremental sync**: OpenSea cursor stored in `sync_state`; resumable and incremental
- **Gas is optional**: `--gas` flag / API checkbox to avoid burning Etherscan quota
- **Unmatched sells** (minted/airdropped NFTs sold without a tracked buy) are excluded from PnL

## API routes
| Method | Path | Description |
|---|---|---|
| GET | `/api/wallets` | All wallets with cached summary stats |
| GET | `/api/report/<address>` | Full analytics for one wallet |
| GET | `/api/trades/<address>` | Raw trades, optional `?collection=` filter |
| POST | `/api/sync` | Trigger sync (SSE stream of log lines) |
| GET | `/api/floor/<address>` | Fetch live floor prices for open positions |
| GET | `/api/meta` | Cross-wallet collection stats, filtered to >10 trades |
| PATCH | `/api/wallet/<address>` | Update wallet name/notes |

## Running
```
pip install -r requirements.txt
# set OPENSEA_API_KEY (and optionally ETHERSCAN_API_KEY) in .env
python app.py          # web UI at http://localhost:5000
python main.py sync 0xWALLET
python main.py report 0xWALLET
```

## Frontend architecture
Single HTML file, no build step. Hash-based router (`#/`, `#/wallet/<addr>`, `#/meta`). Sortable table engine in `tbls` registry — `initTable(id, rows, renderFn, sortCol, sortDir)`. Meta view filters are client-side against a cached `metaCache` array.
