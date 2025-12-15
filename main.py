import requests
import csv
import os
from datetime import datetime, timedelta, timezone

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

JST = timezone(timedelta(hours=9))
TODAY = datetime.now(JST).strftime("%Y-%m-%d")

# =========================
# utils
# =========================
def val(x):
    if isinstance(x, dict):
        return x.get("value")
    return x

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

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
    return {
        "timestamp": datetime.now(JST).isoformat(),
        "airport": name,
        "icao": m.get("station"),
        "temperature_c": val(m.get("temperature")),
        "dewpoint_c": val(m.get("dewpoint")),
        "wind_dir_deg": val(m.get("wind_direction")),
        "wind_speed_kt": val(m.get("wind_speed")),
        "wind_gust_kt": val(m.get("wind_gust")),
        "visibility_m": val(m.get("visibility")),
        "pressure_hpa": val(m.get("altimeter")),
        "weather": " ".join(m.get("wx_codes", [])),
        "clouds": " ".join(
            f"{c['type']}{c.get('altitude','')}"
            for c in m.get("clouds", [])
        ),
        "raw": m.get("raw", "")
    }

# =========================
# TAF
# =========================
def get_taf(icao):
    return fetch(f"{AVWX_BASE}/taf/{icao}")

def expand_taf_hourly(station, taf):
    start = datetime.fromisoformat(taf["valid_time"]["from"])
    end = datetime.fromisoformat(taf["valid_time"]["to"])

    rows = []
    t = start.replace(minute=0, second=0, microsecond=0)

    while t <= end:
        for e in taf["forecast"]:
            e_start = datetime.fromisoformat(e["time"]["from"])
            e_end = datetime.fromisoformat(e["time"].get("to", end.isoformat()))

            if e_start <= t <= e_end:
                rows.append(make_taf_row(station, t, e))

        t += timedelta(hours=1)

    return rows

def make_taf_row(station, t, e):
    wind_dir = val(e.get("wind_direction"))
    wind_spd = val(e.get("wind_speed"))
    gust = val(e.get("wind_gust"))

    wind = ""
    if wind_dir is not None and wind_spd is not None:
        wind = f"{wind_dir:03d}{wind_spd:02d}"
        if gust:
            wind += f"G{gust}"
        wind += "KT"

    raw = e.get("raw")
    if isinstance(raw, list):
        raw = " ".join(raw)
    elif not isinstance(raw, str):
        raw = ""

    return {
        "station": station,
        "forecast_time": t.strftime("%Y-%m-%d %H:%M"),
        "type": e.get("type"),
        "wind": wind,
        "visibility": val(e.get("visibility")),
        "weather": " ".join(e.get("wx_codes", [])),
        "clouds": " ".join(
            f"{c['type']}{c.get('altitude','')}"
            for c in e.get("clouds", [])
        ),
        "raw": raw
    }

# =========================
# CSV
# =========================
def write_csv(path, rows, fields):
    if not rows:
        return
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

# =========================
# main
# =========================
def main():
    for name, icao in AIRPORTS.items():
        base = f"data/{name}"
        ensure_dir(base)

        # METAR
        try:
            m = get_metar(icao)
            row = parse_metar(m, name)
            write_csv(
                f"{base}/metar_{TODAY}.csv",
                [row],
                row.keys()
            )
        except Exception as e:
            print("METAR error:", icao, e)

        # TAF
        try:
            taf = get_taf(icao)
            rows = expand_taf_hourly(name, taf)
            if rows:
                write_csv(
                    f"{base}/taf_{TODAY}.csv",
                    rows,
                    rows[0].keys()
                )
        except Exception as e:
            print("TAF error:", icao, e)

    print("done")

if __name__ == "__main__":
    main()
