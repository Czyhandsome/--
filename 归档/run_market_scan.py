# run_market_scan.py
"""
中长线 A 股市场扫描器（只读本地缓存）

设计目标：
- 不依赖实时行情接口（避免炸）
- 偏中长线（周线级别）
- 低频、稳健、可长期运行
- 只做「过滤」，不做「预测」
"""

from pathlib import Path
from datetime import date
import pandas as pd
import akshare as ak

# =========================
# 1. 全局参数（刻意很少）
# =========================

DATA_DIR = Path("data/stocks")     # update_stock_data.py 生成的缓存
OUTPUT_DIR = Path("market")
OUTPUT_DIR.mkdir(exist_ok=True)

MA_WINDOW = 20          # 周线 MA20 ≈ 5 个月
MIN_LIST_DAYS = 250     # 至少 1 年日线数据
MAX_OUTPUT = 50         # 最多输出 50 只
MIN_UP_COUNT = 1000     # 市场情绪闸门（上涨家数）

today_str = str(date.today())

# =========================
# 2. 市场环境闸门
# =========================

print("🌡 Checking market environment...")

try:
    breadth = ak.stock_market_activity_legu()
    up_row = breadth[breadth["item"] == "上涨"]
    up_cnt = int(up_row["value"].iloc[0]) if not up_row.empty else None
except Exception as e:
    print(f"⚠️ 市场宽度获取失败：{e}")
    up_cnt = None

if up_cnt is not None:
    print(f"📈 上涨家数：{up_cnt}")
    if up_cnt < MIN_UP_COUNT:
        print(f"🚫 市场环境不佳（<{MIN_UP_COUNT}），本次不扫描")
        exit(0)
else:
    print("⚠️ 无法判断市场环境，谨慎放行")

print("✅ 市场环境允许，开始扫描")

# =========================
# 3. 股票列表（只拿代码 & 名称）
# =========================

print("📊 Fetching stock list...")
stock_list = ak.stock_info_a_code_name()

results = []

# =========================
# 4. 个股扫描（完全本地）
# =========================

total = len(stock_list)

for idx, (_, row) in enumerate(stock_list.iterrows(), 1):
    code = row["code"]
    name = row["name"]

    if idx % 100 == 0:
        print(f"⏳ Progress: {idx}/{total}")

    file_path = DATA_DIR / f"{code}.csv"
    if not file_path.exists():
        continue

    try:
        df = pd.read_csv(file_path)
    except Exception:
        continue

    if len(df) < MIN_LIST_DAYS:
        continue

    # -------------------------
    # 基础清洗
    # -------------------------
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # 最近 10 个交易日必须有成交量（防停牌/ST）
    if df.tail(10)["volume"].sum() == 0:
        continue

    # -------------------------
    # 周线 & MA
    # -------------------------
    weekly = df.resample("W", on="date").last()

    if len(weekly) < MA_WINDOW + 1:
        continue

    weekly["ma"] = weekly["close"].rolling(MA_WINDOW).mean()

    this = weekly.iloc[-1]
    last = weekly.iloc[-2]

    # -------------------------
    # 中长线趋势条件（唯一核心）
    # -------------------------
    if this["close"] > this["ma"] and last["close"] <= last["ma"]:
        results.append({
            "code": code,
            "name": name,
            "close": round(this["close"], 2),
            "ma20": round(this["ma"], 2),
            "signal": "WEEKLY_TREND_UP"
        })

    if len(results) >= MAX_OUTPUT:
        break

# =========================
# 5. 输出结果
# =========================

out_df = pd.DataFrame(results)
out_file = OUTPUT_DIR / f"watchlist_{today_str}.csv"
out_df.to_csv(out_file, index=False, encoding="utf-8-sig")

print(f"\n✅ 扫描完成，候选股票：{len(out_df)} 只")
print(f"📁 输出文件：{out_file}")
