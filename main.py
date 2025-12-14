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
# API取得
# =========================
def fetch(endpoint):
    r = requests.get(endpoint, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()

# =========================
# METAR
# =========================
def val(x):
    if isinstance(x, dict):
        return x.get("value")
    return x

def get_metar(icao):
    return fetch(f"{AVWX_BASE}/metar/{icao}")

def parse_metar(m, name):
    raw = m.get("raw", "")

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
        "raw": raw
    }

# =========================
# TAF
# =========================
def get_taf(icao):
    return fetch(f"{AVWX_BASE}/taf/{icao}")

def expand_taf_hourly(station, taf):
    issued = datetime.fromisoformat(taf["time"]["dt"])
    start = datetime.fromisoformat(taf["valid_time"]["from"])
    end = datetime.fromisoformat(taf["valid_time"]["to"])

    rows = []

    t = start.replace(minute=0, second=0, microsecond=0)
    while t <= end:
        base_event = None

        # BASE / FM 優先
        for e in taf["forecast"]:
            e_start = datetime.fromisoformat(e["time"]["from"])
            e_end = datetime.fromisoformat(e["time"].get("to", end.isoformat()))
            if e["type"] in ("BASE", "FM") and e_start <= t <= e_end:
                if not base_event or e_start >= datetime.fromisoformat(base_event["time"]["from"]):
                    base_event = e

        if base_event:
            rows.append(make_taf_row(station, t, base_event, "BASE"))

        # TEMPO は並列
        for e in taf["forecast"]:
            if e["type"] == "TEMPO":
                e_start = datetime.fromisoformat(e["time"]["from"])
                e_end = datetime.fromisoformat(e["time"]["to"])
                if e_start <= t <= e_end:
                    rows.append(make_taf_row(station, t, e, "TEMPO"))

        t += timedelta(hours=1)

    return rows

def make_taf_row(station, t, e, layer):
    def val(x):
        if isinstance(x, dict):
            return x.get("value")
        return x

    wind_dir = val(e.get("wind_direction"))
    wind_spd = val(e.get("wind_speed"))
    gust = val(e.get("wind_gust"))

    wind_str = ""
    if wind_dir is not None and wind_spd is not None:
        wind_str = f"{wind_dir:03d}{wind_spd:02d}"
        if gust:
            wind_str += f"G{gust}"
        wind_str += "KT"

    raw_str = e.get("raw")
    if isinstance(raw_str, list):
        raw_str = " ".join(raw_str)
    elif not isinstance(raw_str, str):
        raw_str = ""

    return {
        "station": station,
        "forecast_time": t.strftime("%Y-%m-%d %H:%M"),
        "layer": layer,
        "wind": wind_str,
        "visibility": val(e.get("visibility")),
        "weather": " ".join(e.get("wx_codes", [])),
        "clouds": " ".join(
            f"{c['type']}{c.get('altitude','')}"
            for c in e.get("clouds", [])
        ),
        "raw": raw_str
    }

# =========================
# CSV
# =========================
def write_csv(filename, rows, fieldnames):
    if not rows:
        return
    write_header = not os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

# =========================
# main
# =========================
def main():
    metar_rows = []
    taf_rows = []

    for name, icao in AIRPORTS.items():
        try:
            m = get_metar(icao)
            metar_rows.append(parse_metar(m, name))
        except Exception as e:
            print("METAR error:", icao, e)

        try:
            taf = get_taf(icao)
            taf_rows.extend(expand_taf_hourly(name, taf))
        except Exception as e:
            print("TAF error:", icao, e)

    metar_fields = ["timestamp","airport","icao","temperature_c","dewpoint_c",
                    "wind_dir_deg","wind_speed_kt","wind_gust_kt","visibility_m",
                    "pressure_hpa","weather","clouds","raw"]
    taf_fields = ["station","forecast_time","layer","wind","visibility",
                  "weather","clouds","raw"]

    write_csv(f"metar_{TODAY}.csv", metar_rows, metar_fields)
    write_csv(f"taf_{TODAY}.csv", taf_rows, taf_fields)

    print("done")

if __name__ == "__main__":
    main()
