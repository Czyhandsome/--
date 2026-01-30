# update_data.py
"""
只更新 universe 内股票的日线数据（Eastmoney 直连版）
- 极低频
- 自动断点
- 失败不影响整体
"""

import time
import requests
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data/stocks")
DATA_DIR.mkdir(parents=True, exist_ok=True)

UNIVERSE_FILE = Path("universe/final_universe.csv")

MAX_PER_RUN = 10
SLEEP_SEC = 5
START_DATE = "20180101"


def fetch_daily_kline_eastmoney(symbol: str, start_date: str) -> dict:
    """直接调用 Eastmoney K 线 API，返回 JSON"""
    market_code = 1 if symbol.startswith("6") else 0

    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        "?fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6"
        "&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61%2Cf116"
        "&ut=7eea3edcaed734bea9cbfc24409ed989"
        "&klt=101"
        "&fqt=0"
        f"&secid={market_code}.{symbol}"
        f"&beg={start_date}"
        "&end=20500101"
    )

    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def parse_eastmoney_klines_to_df(raw: dict) -> pd.DataFrame:
    """解析 Eastmoney klines → 标准 DataFrame"""
    data = raw.get("data")
    if not data or "klines" not in data:
        return pd.DataFrame()

    rows = []
    for line in data["klines"]:
        parts = line.split(",")
        if len(parts) < 6:
            continue

        rows.append({
            "date": parts[0],
            "open": float(parts[1]),
            "close": float(parts[2]),
            "high": float(parts[3]),
            "low": float(parts[4]),
            "volume": float(parts[5]),
        })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    universe = pd.read_csv(UNIVERSE_FILE, dtype=str)
    processed = 0

    print(f"📊 Universe 股票数：{len(universe)}")

    for _, row in universe.iterrows():
        if processed >= MAX_PER_RUN:
            break

        code = row["code"]
        file_path = DATA_DIR / f"{code}.csv"

        print(f"\n🔄 更新 {code}")

        # -------------------------
        # 断点判断
        # -------------------------
        if file_path.exists():
            try:
                existing = pd.read_csv(file_path)
                last_date = existing["date"].max()
                start_date = last_date.replace("-", "")
            except Exception:
                start_date = START_DATE
        else:
            start_date = START_DATE

        today_str = pd.Timestamp.today().strftime("%Y%m%d")
        if start_date >= today_str:
            print("⚠️ 数据已是最新，无需更新")
            continue

        # -------------------------
        # 拉取 + 解析
        # -------------------------
        try:
            raw = fetch_daily_kline_eastmoney(code, start_date)
            df = parse_eastmoney_klines_to_df(raw)
        except Exception as e:
            print(f"❌ 拉取失败: {e}")
            time.sleep(SLEEP_SEC)
            continue

        if df.empty:
            print("⚠️ 无新数据")
            time.sleep(SLEEP_SEC)
            continue

        # -------------------------
        # 合并 & 保存
        # -------------------------
        if file_path.exists():
            combined = (
                pd.concat([existing, df])
                .drop_duplicates(subset=["date"])
                .sort_values("date")
            )
        else:
            combined = df.sort_values("date")

        combined.to_csv(file_path, index=False)
        print(f"✅ 更新完成，共 {len(combined)} 行")

        processed += 1
        time.sleep(SLEEP_SEC)

    print(f"\n🎯 本次更新完成：{processed} 只")
