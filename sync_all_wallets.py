"""Sync all wallets sequentially. Run directly or via Task Scheduler."""

import os
import subprocess
import sys
import time
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def get_wallets():
    import db
    db.init_db()
    with db.get_conn() as conn:
        rows = conn.execute("SELECT address, name FROM wallets ORDER BY rowid").fetchall()
    return [(r["address"], r["name"]) for r in rows]


def sync_wallet(address: str, label: str) -> bool:
    """Run `main.py sync <address>`. Returns True on success."""
    cmd = [sys.executable, "-u", "main.py", "sync", address]
    log.info("Syncing %s (%s)", label, address)
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=os.environ.copy(),
        )
        elapsed = time.time() - t0
        for line in result.stdout.splitlines():
            if line.strip():
                log.info("  %s", line.strip())
        if result.returncode != 0:
            log.error("  FAILED (exit %d) in %.1fs", result.returncode, elapsed)
            if result.stderr:
                for line in result.stderr.splitlines():
                    log.error("  stderr: %s", line)
            return False
        log.info("  Done in %.1fs", elapsed)
        return True
    except Exception as exc:
        log.error("  Exception syncing %s: %s", address, exc)
        return False


def main():
    if not os.environ.get("OPENSEA_API_KEY"):
        log.error("OPENSEA_API_KEY not set. Add it to .env.")
        sys.exit(1)

    wallets = get_wallets()
    if not wallets:
        log.info("No wallets found in database.")
        return

    log.info("Starting sync for %d wallet(s) at %s",
             len(wallets), datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))

    ok = failed = 0
    for address, name in wallets:
        label = name or address[:10] + "..."
        if sync_wallet(address, label):
            ok += 1
        else:
            failed += 1

    log.info("Finished: %d succeeded, %d failed.", ok, failed)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
