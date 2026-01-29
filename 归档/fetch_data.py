# fetch_data.py
import akshare as ak
import pandas as pd
from pathlib import Path

symbols = [
    "510300",
    "159919",
    "512100",
    "513100",
]

output_path = Path("data")
output_path.mkdir(exist_ok=True)

for symbol in symbols:
    print(f"📥 Fetching {symbol} ...")

    df = ak.fund_etf_hist_em(
        symbol=symbol,
        period="daily",
        adjust=""
    )

    if df is None or df.empty:
        print(f"❌ {symbol} 数据为空，跳过")
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
    df.to_csv(output_path / f"{symbol}.csv", index=False)

    print(f"✅ data/{symbol}.csv 已生成")
