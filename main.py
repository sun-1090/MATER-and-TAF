import requests
import csv
import os
from datetime import datetime, timezone

# =========================
# 空港
# =========================
AIRPORTS = {
    "NRT": "RJAA",
    "HND": "RJTT",
    "TSN": "ZBTJ",
    "DLC": "ZYTL",
    "SJW": "ZBSJ"
}

AVWX_BASE = "https://avwx.rest/api"
TOKEN = os.environ.get("AVWX_TOKEN")

HEADERS = {}
if TOKEN:
    HEADERS["Authorization"] = f"token {TOKEN}"

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# =========================
# API取得
# =========================
def fetch(endpoint):
    r = requests.get(endpoint, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()

# =========================
# METAR
# =========================
def get_metar(icao):
    return fetch(f"{AVWX_BASE}/metar/{icao}")

def parse_metar(m, name):
    raw = m.get("raw", "")
    wind = m.get("wind_direction")
    wind_spd = m.get("wind_speed")
    gust = m.get("wind_gust")

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "airport": name,
        "icao": m.get("station"),
        "temperature_c": m.get("temperature"),
        "dewpoint_c": m.get("dewpoint"),
        "wind_dir_deg": wind,
        "wind_speed_kt": wind_spd,
        "wind_gust_kt": gust,
        "visibility_m": m.get("visibility"),
        "pressure_hpa": m.get("altimeter"),
        "weather": " ".join(m.get("wx_codes", [])),
        "clouds": " ".join(f"{c['type']}{c.get('altitude','')}" for c in m.get("clouds", [])),
        "raw": raw
    }

# =========================
# CSV 書き込み
# =========================
def write_csv(filename, rows):
    if not rows:
        return
    write_header = not os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

# =========================
# main
# =========================
def main():
    metar_rows = []

    for name, icao in AIRPORTS.items():
        try:
            m = get_metar(icao)
            metar_rows.append(parse_metar(m, name))
        except Exception as e:
            print("METAR error:", icao, e)

    write_csv(f"metar_{TODAY}.csv", metar_rows)
    print("done")

if __name__ == "__main__":
    main()
