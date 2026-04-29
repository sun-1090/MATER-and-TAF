import requests
import csv
import os
from datetime import datetime, timedelta, timezone

# =========================
# 設定：空港リスト
# =========================
AIRPORTS = {
    "成田": "RJAA",
    "羽田": "RJTT",
    "天津": "ZBTJ",
    "大連": "ZYTL",
    "石家荘": "ZBSJ"
}

AVWX_BASE = "https://avwx.rest/api"
TOKEN = os.environ.get("AVWX_TOKEN")

HEADERS = {}
if TOKEN:
    HEADERS["Authorization"] = f"token {TOKEN}"

JST = timezone(timedelta(hours=9))
TODAY = datetime.now(JST).strftime("%Y-%m-%d")

# =========================
# ユーティリティ
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

def parse_iso(iso_str):
    """ISO 8601文字列をdatetimeに変換。Zを+00:00に置換して安定化。"""
    if not iso_str:
        return None
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))

# =========================
# METAR
# =========================
def get_metar(icao):
    return fetch(f"{AVWX_BASE}/metar/{icao}")

def parse_metar(m, name):
    return {
        "取得日時": datetime.now(JST).isoformat(),
        "空港名": name,
        "ICAOコード": m.get("station"),
        "気温(°C)": val(m.get("temperature")),
        "露点温度(°C)": val(m.get("dewpoint")),
        "風向(°)": val(m.get("wind_direction")),
        "風速(kt)": val(m.get("wind_speed")),
        "最大瞬間風速(kt)": val(m.get("wind_gust")),
        "視程(m)": val(m.get("visibility")),
        "気圧(hPa)": val(m.get("altimeter")),
        "現在天気": " ".join(m.get("wx_codes", []) if m.get("wx_codes") else []),
        "雲の状態": " ".join(f"{c['type']}{c.get('altitude','')}" for c in m.get("clouds", [])) if m.get("clouds") else "",
        "原文": m.get("raw", "")
    }

# =========================
# TAF
# =========================
def get_taf(icao):
    return fetch(f"{AVWX_BASE}/taf/{icao}")

def expand_taf_hourly(station_name, taf):
    """予報を1時間刻みに展開"""
    # エラーの元: 使うキー(start_time/end_time)をしっかり指定
    if not taf or "start_time" not in taf or "end_time" not in taf:
        print(f"Warning: {station_name} のデータに有効期間情報がありません。")
        return []

    try:
        # valid_time ではなく start_time/end_time を使用
        start = parse_iso(taf["start_time"].get("dt"))
        end = parse_iso(taf["end_time"].get("dt"))
    except Exception as e:
        print(f"Error Parsing time for {station_name}: {e}")
        return []
    
    if not start or not end:
        return []

    rows = []
    t = start.replace(minute=0, second=0, microsecond=0)

    while t <= end:
        target_forecast = None
        # 各時刻において、最適な予報セグメント(forecastの中身)を検索
        for e in taf.get("forecast", []):
            e_start_str = e.get("start_time", {}).get("dt")
            e_end_str = e.get("end_time", {}).get("dt")
            
            if not e_start_str:
                continue
            
            e_start = parse_iso(e_start_str)
            e_end = parse_iso(e_end_str) if e_end_str else end

            if e_start <= t <= e_end:
                target_forecast = e
                # BECMGなどの後続の変化を拾うため最後まで回す

        if target_forecast:
            rows.append(make_taf_row(station_name, t, target_forecast))
        
        t += timedelta(hours=1)
    return rows

def make_taf_row(station_name, t, e):
    wind_dir = val(e.get("wind_direction"))
    wind_spd = val(e.get("wind_speed"))
    gust = val(e.get("wind_gust"))

    wind = ""
    if wind_dir is not None and wind_spd is not None:
        try:
            wind = f"{int(wind_dir):03d}{int(wind_spd):02d}"
            if gust: wind += f"G{int(gust)}"
            wind += "KT"
        except:
            wind = "VRB"

    raw = e.get("raw")
    if isinstance(raw, list): raw = " ".join(raw)
    elif not isinstance(raw, str): raw = ""

    return {
        "空港名": station_name,
        "予報時刻": t.astimezone(JST).strftime("%Y-%m-%d %H:%M"),
        "変化の種類": e.get("type"),
        "風情報": wind,
        "視程": val(e.get("visibility")),
        "天気": " ".join(e.get("wx_codes", []) if e.get("wx_codes") else []),
        "雲の状態": " ".join(f"{c['type']}{c.get('altitude','')}" for c in e.get("clouds", [])) if e.get("clouds") else "",
        "原文セグメント": raw
    }

# =========================
# CSV出力
# =========================
def write_csv(path, rows, fields):
    if not rows: return
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header: writer.writeheader()
        writer.writerows(rows)

# =========================
# メイン処理
# =========================
def main():
    for name, icao in AIRPORTS.items():
        base = f"data/{name}"
        ensure_dir(base)

        # METAR
        try:
            m = get_metar(icao)
            row = parse_metar(m, name)
            write_csv(f"{base}/metar_{TODAY}.csv", [row], row.keys())
            print(f"成功: {name} METAR")
        except Exception as e:
            print(f"METARエラー ({name}/{icao}):", e)

        # TAF
        try:
            taf_data = get_taf(icao)
            rows = expand_taf_hourly(name, taf_data)
            if rows:
                write_csv(f"{base}/taf_{TODAY}.csv", rows, rows[0].keys())
                print(f"成功: {name} TAF")
            else:
                print(f"データなし: {name} TAF")
        except Exception as e:
            print(f"TAFエラー ({name}/{icao}):", e)

    print("\nすべての処理が完了しました。")

if __name__ == "__main__":
    main()
