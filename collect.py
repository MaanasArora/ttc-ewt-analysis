import httpx
import time
from datetime import datetime
from pathlib import Path

URL = "https://gtfsrt.ttc.ca/vehicles/position?format=binary"
OUTDIR = Path("../ttc_data")
OUTDIR.mkdir(exist_ok=True)

while True:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = OUTDIR / f"vehicle_{ts}.pb"

    try:
        r = httpx.get(URL, timeout=10)
        r.raise_for_status()
        filename.write_bytes(r.content)
        print("saved", filename, "at", ts)
    except Exception as e:
        print("error:", e)

    time.sleep(15)
