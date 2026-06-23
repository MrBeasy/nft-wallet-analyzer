"""Quick debug script to inspect raw OpenSea API response."""
import os, json, requests
from dotenv import load_dotenv
load_dotenv()

WALLET = "0x2cC0C55372416b06eC871a3f18E2aCE0741CCeA3"
KEY = os.environ.get("OPENSEA_API_KEY", "")
BASE = "https://api.opensea.io/api/v2"

headers = {"accept": "application/json", "x-api-key": KEY}

# 1. Try events endpoint with event_type=sale
url = f"{BASE}/events/accounts/{WALLET}"
params = {"event_type": "sale", "chain": "ethereum", "limit": 5}
print(f"GET {url}")
print(f"Params: {params}\n")
r = requests.get(url, params=params, headers=headers, timeout=15)
print(f"Status: {r.status_code}")
data = r.json()
print(json.dumps(data, indent=2)[:3000])

print("\n--- First event payment token (if any) ---")
events = data.get("asset_events", [])
for ev in events[:2]:
    payment = ev.get("payment") or {}
    token = payment.get("token") or {}
    print("payment:", json.dumps(payment, indent=2))
    print("nft:", json.dumps(ev.get("nft"), indent=2))
    print("seller:", ev.get("seller"))
    print("buyer:", ev.get("buyer"))
    print()
