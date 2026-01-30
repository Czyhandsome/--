import tushare as ts
import pandas as pd
from pathlib import Path
import time
import os

# ========== 配置 ==========
TS_TOKEN = os.getenv("TUSHARE_TOKEN")
UNIVERSE_FILE = "universe/final_universe.csv"
DATA_DIR = Path("data/stocks")
DATA_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "20180101"
SLEEP_SEC = 0.6   # TuShare 免费限频

# ========== 初始化 ==========
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

universe = pd.read_csv(UNIVERSE_FILE, dtype={"code": str})
print(f"📊 Universe 股票数：{len(universe)}")

# ========== 主循环 ==========
for _, row in universe.iterrows():
    code = row["code"].zfill(6)
    name = row.get("name", "")

    ts_code = f"{code}.SH" if code.startswith("6") else f"{code}.SZ"
    out_file = DATA_DIR / f"{ts_code}.csv"

    print(f"\n🔄 更新 {ts_code} {name}")

    try:
        df = ts.pro_bar(
            ts_code=ts_code,
            asset="E",
            adj=None,
            freq="D",
            start_date=START_DATE
        )

        if df is None or df.empty:
            print("⚠️ 无数据，跳过")
            continue

        df = df.sort_values("trade_date")
        df.rename(columns={
            "trade_date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "vol": "volume"
        }, inplace=True)

        df["date"] = pd.to_datetime(df["date"])
        df = df[["date", "open", "high", "low", "close", "volume"]]

        df.to_csv(out_file, index=False)
        print(f"✅ 更新完成：{len(df)} 行")

    except Exception as e:
        print(f"❌ 拉取失败: {e}")

    time.sleep(SLEEP_SEC)
