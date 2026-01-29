# run_market_scan.py
import akshare as ak
import pandas as pd
from pathlib import Path
from datetime import date

# =========================
# 参数（偏中长线，极稳）
# =========================

MA_WINDOW = 20          # 周线 MA20 ≈ 5 个月
MIN_LIST_DAYS = 250     # 至少上市一年
MAX_OUTPUT = 50         # 最多输出 50 只
MIN_SHANGZHANG = 1000   # 上涨家数阈值

OUTPUT_DIR = Path("market")
OUTPUT_DIR.mkdir(exist_ok=True)

today_str = str(date.today())

print("📊 Fetching A-share list...")
stock_list = ak.stock_info_a_code_name()

results = []

# =========================
# 市场环境（简化版）
# =========================

print("🌡 Checking market environment...")

breadth = ak.stock_market_activity_legu()

# 从 item/value 结构中取“上涨家数”
up_row = breadth[breadth["item"] == "上涨"]

if up_row.empty:
    print("⚠️ 未找到“上涨”数据，跳过市场闸门")
    up_cnt = 9999  # 放行，但给警告
else:
    up_cnt = int(up_row["value"].iloc[0])

print(f"📈 上涨家数：{up_cnt}")

if up_cnt < MIN_SHANGZHANG:
    print(f"🚫 市场环境不佳（上涨 {up_cnt} 家小于阈值 {MIN_SHANGZHANG}），停止扫描")
    exit(0)

print("✅ 市场环境允许，继续扫描")

# =========================
# 个股扫描
# =========================

print("📥 Fetching daily snapshot...")
spot = ak.stock_zh_a_spot_em()

# 只保留最近有交易、非 ST、非停牌
valid_codes = set(
    spot[
        (spot["成交量"] > 0) &
        (~spot["名称"].str.contains("ST"))
    ]["代码"]
)

total = len(stock_list)

for i, (_, row) in enumerate(stock_list.iterrows(), 1):
    if i % 50 == 0:
        print(f"⏳ Progress: {i}/{total}")
    code = row["code"]

    if code not in valid_codes:
        continue
    name = row["name"]

    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date="20220101",
            adjust=""
        )
    except Exception:
        continue

    if df is None or len(df) < MIN_LIST_DAYS:
        continue

    df = df.rename(columns={
        "日期": "date",
        "收盘": "close",
    })

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # =========================
    # 周线处理
    # =========================

    weekly = df.resample("W", on="date").last()
    weekly["ma"] = weekly["close"].rolling(MA_WINDOW).mean()

    if len(weekly) < MA_WINDOW + 1:
        continue

    this = weekly.iloc[-1]
    last = weekly.iloc[-2]

    # =========================
    # 中长线趋势条件
    # =========================

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
# 输出
# =========================

out_df = pd.DataFrame(results)
out_file = OUTPUT_DIR / f"watchlist_{today_str}.csv"
out_df.to_csv(out_file, index=False, encoding="utf-8-sig")

print(f"✅ 输出候选股票 {len(out_df)} 只")
print(f"📁 文件位置：{out_file}")
