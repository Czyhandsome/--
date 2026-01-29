# generate_universe.py
"""
生成核心股票池（universe）
- 沪深300
- 中证红利
（稳健适配 AkShare 中文列名）
"""

from pathlib import Path
import akshare as ak
import pandas as pd

UNIVERSE_DIR = Path("universe")
UNIVERSE_DIR.mkdir(exist_ok=True)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    code_col = None
    name_col = None

    for col in df.columns:
        if col.endswith("代码"):
            code_col = col
        if col.endswith("名称") or col.endswith("简称"):
            name_col = col

    if code_col is None or name_col is None:
        raise ValueError(f"无法识别代码/名称列: {df.columns}")

    return (
        df[[code_col, name_col]]
        .rename(columns={code_col: "code", name_col: "name"})
        .astype(str)
    )

# =========================
# 1. 沪深300
# =========================

print("📥 Fetching HS300 constituents...")
hs300_raw = ak.index_stock_cons(symbol="000300")
hs300_df = normalize_columns(hs300_raw)

hs300_file = UNIVERSE_DIR / "hs300.csv"
hs300_df.to_csv(hs300_file, index=False, encoding="utf-8-sig")

print(f"✅ HS300 成分股：{len(hs300_df)} 只")
print(f"📁 输出：{hs300_file}")

# =========================
# 2. 中证红利
# =========================

print("\n📥 Fetching CSI Dividend constituents...")
div_raw = ak.index_stock_cons(symbol="000922")
div_df = normalize_columns(div_raw)

div_file = UNIVERSE_DIR / "dividend.csv"
div_df.to_csv(div_file, index=False, encoding="utf-8-sig")

print(f"✅ 中证红利成分股：{len(div_df)} 只")
print(f"📁 输出：{div_file}")

print("\n🎯 Universe 生成完成")
