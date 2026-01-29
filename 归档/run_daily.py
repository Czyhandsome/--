# run_daily.py
import pandas as pd
from datetime import date
from pathlib import Path
import json

symbols = [
    "510300",
    "159919",
    "512100",
    "513100",
]

# =========================
# 全局策略参数（统一）
# =========================

MA_WINDOW = 20
RISK_PER_TRADE = 100
STOP_LOSS_PCT = 0.02

DATA_DIR = Path("data")
OUTPUT_DIR = Path("orders")
OUTPUT_DIR.mkdir(exist_ok=True)

today_str = str(date.today())

for SYMBOL in symbols:
    print(f"\n🔍 Processing {SYMBOL}")

    data_path = DATA_DIR / f"{SYMBOL}.csv"
    if not data_path.exists():
        print(f"❌ 缺少数据文件：{data_path}")
        continue

    # =========================
    # 1. 加载数据
    # =========================

    df = pd.read_csv(data_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    if len(df) < MA_WINDOW + 1:
        print("❌ 数据不足，无法计算 MA")
        continue

    # =========================
    # 2. 计算指标
    # =========================

    df["ma"] = df["close"].rolling(MA_WINDOW).mean()

    today = df.iloc[-1]
    yesterday = df.iloc[-2]

    # =========================
    # 3. 信号判断
    # =========================

    signal = "HOLD"

    if today["close"] > today["ma"] and yesterday["close"] <= yesterday["ma"]:
        signal = "BUY"
    elif today["close"] < today["ma"] and yesterday["close"] >= yesterday["ma"]:
        signal = "SELL"

    # =========================
    # 4. 风险与仓位
    # =========================

    price = today["close"]
    stop_price = round(price * (1 - STOP_LOSS_PCT), 3)
    risk_per_share = price - stop_price

    if risk_per_share <= 0:
        print("❌ 风险计算异常")
        continue

    max_qty = int(RISK_PER_TRADE / risk_per_share)
    max_qty = (max_qty // 100) * 100

    # =========================
    # 5. 生成指令
    # =========================

    order = {
        "trade_date": today_str,
        "symbol": SYMBOL,
        "signal": signal,
        "price_reference": round(price, 3),
        "price_range": [
            round(price * 0.995, 3),
            round(price * 1.005, 3)
        ],
        "stop_loss": stop_price,
        "quantity": max_qty if signal != "HOLD" else 0,
        "ma_window": MA_WINDOW,
        "reason": "Close/MA cross",
    }

    output_file = OUTPUT_DIR / f"{SYMBOL}_{today_str}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(order, f, indent=2, ensure_ascii=False)

    # =========================
    # 6. 人类可读输出
    # =========================

    print("========== DAILY SIGNAL ==========")
    print(f"Symbol : {SYMBOL}")
    print(f"Signal : {signal}")
    if signal != "HOLD":
        print(f"Qty    : {max_qty}")
        print(f"Price  : {order['price_range']}")
        print(f"Stop   : {stop_price}")
    else:
        print("No action today.")
    print("==================================")
