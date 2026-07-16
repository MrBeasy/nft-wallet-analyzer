"""
One-time migration: re-fetch all collection fees from OpenSea and correct
any stale creator_fee_bps values where optional fees were previously stored
as mandatory.

Run on the server after git pull:
    /opt/nft-analysis/venv/bin/python /opt/nft-analysis/migrate_fix_fees.py
"""

import sys
import time
sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv()

import db
from fetch import fetch_collection_info

conn = db.get_conn()
rows = conn.execute(
    "SELECT contract_address, slug, name, creator_fee_bps, opensea_fee_bps "
    "FROM collections WHERE creator_fee_bps > 0 ORDER BY slug"
).fetchall()

print(f"Checking {len(rows)} collections with non-zero creator fees...")

updated = []
failed = []

for i, row in enumerate(rows, 1):
    slug = row["slug"]
    try:
        info = fetch_collection_info(slug)
        new_creator = info["creator_fee_bps"]
        new_os = info["opensea_fee_bps"]
        new_total = new_creator + new_os
        if new_creator != row["creator_fee_bps"]:
            conn.execute(
                "UPDATE collections SET creator_fee_bps=?, opensea_fee_bps=?, total_fee_bps=? "
                "WHERE contract_address=?",
                (new_creator, new_os, new_total, row["contract_address"]),
            )
            updated.append({"slug": slug, "old": row["creator_fee_bps"], "new": new_creator})
            print(f"  CHANGED {slug}: creator {row['creator_fee_bps']} -> {new_creator} bps (total {new_total})")
        if i % 50 == 0:
            print(f"[{i}/{len(rows)}] ...")
    except Exception as e:
        failed.append((slug, str(e)))

conn.commit()
conn.close()

print(f"\nDone. {len(updated)} updated, {len(failed)} failed.")
if failed:
    print("Failed (likely deleted from OpenSea):")
    for s, e in failed:
        print(f"  {s}: {e}")
