# update_data.py
"""
只更新 universe 内股票的日线数据
- 极低频
- 自动断点
- 失败不影响整体
"""

from pathlib import Path
import time
import pandas as pd
import akshare as ak

DATA_DIR = Path("data/stocks")
DATA_DIR.mkdir(parents=True, exist_ok=True)

UNIVERSE_FILE = Path("universe/final_universe.csv")

MAX_PER_RUN = 10        # 每次最多更新 10 只（非常保守）
SLEEP_SEC = 5           # 每只之间休眠
START_DATE = "20180101"

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
    # 判断是否已有数据
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

    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            adjust=""
        )
    except Exception as e:
        print(f"❌ 拉取失败: {e}")
        time.sleep(SLEEP_SEC)
        continue

    if df is None or df.empty:
        print("⚠️ 无新数据")
        time.sleep(SLEEP_SEC)
        continue

    df = df.rename(columns={
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
    })

    df = df[["date", "open", "high", "low", "close", "volume"]]

    if file_path.exists():
        combined = pd.concat([existing, df]).drop_duplicates(subset=["date"])
    else:
        combined = df

    combined = combined.sort_values("date")
    combined.to_csv(file_path, index=False)

    print(f"✅ 更新完成，共 {len(combined)} 行")

    processed += 1
    time.sleep(SLEEP_SEC)

print(f"\n🎯 本次更新完成：{processed} 只")
