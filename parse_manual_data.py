# parse_manual_data.py
"""
解析 data/manual/stocks 下的 Eastmoney JSON
并合并更新到 data/stocks/{code}.csv

- 自动检测所有 json
- 自动断点
- 幂等（可反复跑）
- 单股票失败不影响整体
"""

import json
from pathlib import Path

import pandas as pd

MANUAL_DIR = Path("data/manual/stocks")
DATA_DIR = Path("data/stocks")

MANUAL_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


def parse_eastmoney_json(json_path: Path) -> pd.DataFrame:
    """解析单个 Eastmoney kline JSON"""
    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data = raw.get("data")
    if not data or "klines" not in data:
        raise ValueError("JSON 中无 klines 数据")

    records = []
    for line in data["klines"]:
        parts = line.split(",")
        if len(parts) < 6:
            continue

        records.append({
            "date": parts[0],
            "open": float(parts[1]),
            "close": float(parts[2]),
            "high": float(parts[3]),
            "low": float(parts[4]),
            "volume": float(parts[5]),
        })

    return pd.DataFrame(records)


if __name__ == "__main__":
    json_files = sorted(MANUAL_DIR.glob("*.json"))

    print(f"📂 发现手工数据文件：{len(json_files)}")

    for json_path in json_files:
        code = json_path.stem
        csv_path = DATA_DIR / f"{code}.csv"

        print(f"\n🔄 处理 {code}")

        try:
            df_new = parse_eastmoney_json(json_path)
        except Exception as e:
            print(f"❌ JSON 解析失败: {e}")
            continue

        if df_new.empty:
            print("⚠️ 无有效数据")
            continue

        # -------------------------
        # 合并已有数据
        # -------------------------
        if csv_path.exists():
            try:
                df_old = pd.read_csv(csv_path)
                combined = (
                    pd.concat([df_old, df_new])
                    .drop_duplicates(subset=["date"])
                    .sort_values("date")
                )
            except Exception as e:
                print(f"⚠️ 旧 CSV 读取失败，直接覆盖: {e}")
                combined = df_new.sort_values("date")
        else:
            combined = df_new.sort_values("date")

        combined.to_csv(csv_path, index=False)
        print(f"✅ 更新完成，共 {len(combined)} 行")

    print("\n🎯 手工数据解析完成")
