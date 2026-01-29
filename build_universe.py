# build_universe.py
"""
合并 & 清洗 universe
- HS300 ∪ 中证红利
- 去重
- 去 ST
- 输出 final_universe.csv
"""

from pathlib import Path
import pandas as pd
import akshare as ak

UNIVERSE_DIR = Path("universe")
OUTPUT_FILE = UNIVERSE_DIR / "final_universe.csv"

# =========================
# 1. 读取基础 universe
# =========================

hs300 = pd.read_csv(UNIVERSE_DIR / "hs300.csv", dtype=str)
dividend = pd.read_csv(UNIVERSE_DIR / "dividend.csv", dtype=str)

df = pd.concat([hs300, dividend], ignore_index=True)

# 去重（以 code 为准）
df = df.drop_duplicates(subset=["code"]).reset_index(drop=True)

print(f"📦 合并后股票数（未清洗）：{len(df)}")

# =========================
# 2. 去 ST（用名称规则，简单有效）
# =========================

df = df[~df["name"].str.contains("ST")]

print(f"🧹 去 ST 后股票数：{len(df)}")

# =========================
# 3. 排序 & 输出
# =========================

df = df.sort_values("code")

df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

print(f"\n✅ 最终 universe 生成完成")
print(f"📁 文件：{OUTPUT_FILE}")
print(f"🎯 最终股票数：{len(df)}")
