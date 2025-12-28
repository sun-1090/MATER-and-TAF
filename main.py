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
TOKEN = os.environ.get("AVWX_TOKEN")  # 環境変数からトークン取得

HEADERS = {}
if TOKEN:
    HEADERS["Authorization"] = f"token {TOKEN}"

JST = timezone(timedelta(hours=9))
TODAY = datetime.now(JST).strftime("%Y-%m-%d")

# =========================
# ユーティリティ
# =========================
def val(x):
    """辞書から値を取り出す補助関数"""
    if isinstance(x, dict):
        return x.get("value")
    return x

def ensure_dir(path):
    """フォルダが存在しない場合は作成する"""
    if not os.path.exists(path):
        os.makedirs(path)

def fetch(endpoint):
    """APIからデータを取得する"""
    r = requests.get(endpoint, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()

# =========================
# METAR（定時航空気象実況）
# =========================
def get_metar(icao):
    return fetch(f"{AVWX_BASE}/metar/{icao}")

def parse_metar(m, name):
    """METARデータを日本語キーの辞書に変換"""
    return {
        "取得日時": datetime.now(JST).isoformat(),
        "空港名": name,
        "ICAOコード": m.get("station"),
        "気温": val(m.get("temperature")),
        "露点温度": val(m.get("dewpoint")),
        "風向": val(m.get("wind_direction")),
        "風速(kt)": val(m.get("wind_speed")),
        "最大瞬間風速(kt)": val(m.get("wind_gust")),
        "視程(m)": val(m.get("visibility")),
        "気圧(hPa)": val(m.get("altimeter")),
        "現在天気": " ".join(m.get("wx_codes", [])),
        "雲の状態": " ".join(
            f"{c['type']}{c.get('altitude','')}"
            for c in m.get("clouds", [])
        ),
        "原文": m.get("raw", "")
    }

# =========================
# TAF（飛行場予報）
# =========================
def get_taf(icao):
    return fetch(f"{AVWX_BASE}/taf/{icao}")

def expand_taf_hourly(station_name, taf):
    """予報期間を1時間ごとの行に展開する"""
    start = datetime.fromisoformat(taf["valid_time"]["from"])
    end = datetime.fromisoformat(taf["valid_time"]["to"])

    rows = []
    t = start.replace(minute=0, second=0, microsecond=0)

    while t <= end:
        for e in taf["forecast"]:
            e_start = datetime.fromisoformat(e["time"]["from"])
            e_end = datetime.fromisoformat(e["time"].get("to", end.isoformat()))

            if e_start <= t <= e_end:
                rows.append(make_taf_row(station_name, t, e))

        t += timedelta(hours=1)

    return rows

def make_taf_row(station_name, t, e):
    """TAFの各予報時間を日本語キーの辞書に変換"""
    wind_dir = val(e.get("wind_direction"))
    wind_spd = val(e.get("wind_speed"))
    gust = val(e.get("wind_gust"))

    wind = ""
    if wind_dir is not None and wind_spd is not None:
        # 風向を3桁、風速を2桁で整形 (例: 27015G25KT)
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
        "空港名": station_name,
        "予報時刻": t.strftime("%Y-%m-%d %H:%M"),
        "変化の種類": e.get("type"),
        "風情報": wind,
        "視程": val(e.get("visibility")),
        "天気": " ".join(e.get("wx_codes", [])),
        "雲の状態": " ".join(
            f"{c['type']}{c.get('altitude','')}"
            for c in e.get("clouds", [])
        ),
        "原文セグメント": raw
    }

# =========================
# CSV出力
# =========================
def write_csv(path, rows, fields):
    if not rows:
        return
    # ファイルが存在しない場合のみヘッダーを書き込む
    write_header = not os.path.exists(path)
    # utf-8-sig を指定することでExcelでの文字化けを防止
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

# =========================
# メイン処理
# =========================
def main():
    for name, icao in AIRPORTS.items():
        base = f"data/{name}"
        ensure_dir(base)

        # --- METARの処理 ---
        try:
            m = get_metar(icao)
            row = parse_metar(m, name)
            write_csv(
                f"{base}/metar_{TODAY}.csv",
                [row],
                row.keys()
            )
            print(f"成功: {name} METAR")
        except Exception as e:
            print(f"METARエラー ({name}/{icao}):", e)

        # --- TAFの処理 ---
        try:
            taf = get_metar(icao) # 注意：APIのエンドポイントに合わせて修正が必要な場合はここを確認
            # 正しくは get_taf(icao)
            taf_data = get_taf(icao)
            rows = expand_taf_hourly(name, taf_data)
            if rows:
                write_csv(
                    f"{base}/taf_{TODAY}.csv",
                    rows,
                    rows[0].keys()
                )
                print(f"成功: {name} TAF")
        except Exception as e:
            print(f"TAFエラー ({name}/{icao}):", e)

    print("\nすべての処理が完了しました。")

if __name__ == "__main__":
    main()
