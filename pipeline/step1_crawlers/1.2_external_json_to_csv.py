# -*- coding: utf-8 -*-
"""
STEP 1.2 – RAW DATA NORMALIZATION (JSON → CSV)

Purpose:
- Chuẩn hóa TOÀN BỘ data raw về dạng CSV
- STREAMING JSON (không load toàn bộ vào RAM)
- Là bước CHỐT của STEP 1 trước khi sang STEP 2

IMPORTANT RULES:
1. Chỉ xử lý data_raw/external
2. Giữ nguyên tên file source khi xuất CSV
3. Không enrich, không normalize enum
4. Missing value → "__NA__"
"""

import json
import re
import pandas as pd
import ijson
from pathlib import Path

# ==================================================
# PATHS
# ==================================================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_raw" / "external"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s1_data_extracted"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================
# CORE
# ==================================================

def sanitize_mongo_json_line(line: str) -> str:
    """
    Convert Mongo shell syntax to valid JSON
    Example:
    ObjectId("abc") -> "abc"
    """
    line = re.sub(r'ObjectId\("([^"]+)"\)', r'"\1"', line)
    return line.strip()

def process_json_file(file_path: Path, chunk_size: int = 10_000):
    print(f"▶ Processing: {file_path.name}")

    output_path = OUTPUT_DIR / f"{file_path.stem}.csv"
    buffer = []
    batch_index = 0
    total_rows = 0

    def flush_buffer():
        nonlocal buffer, batch_index, total_rows
        if not buffer:
            return
        df = pd.DataFrame(buffer).fillna("__NA__")
        df.to_csv(
            output_path,
            mode="w" if batch_index == 0 else "a",
            header=(batch_index == 0),
            index=False,
            encoding="utf-8-sig"
        )
        total_rows += len(df)
        batch_index += 1
        buffer.clear()
        print(f"  ✓ Batch {batch_index} written ({total_rows} rows)")

    try:
        # =========================
        # TRY 1: JSON ARRAY STREAM
        # =========================
        try:
            with open(file_path, "rb") as f:
                for item in ijson.items(f, "item"):
                    if not isinstance(item, dict):
                        continue
                    row = {
                        k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                        for k, v in item.items()
                    }
                    buffer.append(row)
                    if len(buffer) >= chunk_size:
                        flush_buffer()
            flush_buffer()
            if total_rows == 0:
                print("⚠️  No rows parsed as JSON → fallback to TSV")
                parse_tsv_fallback(file_path)
                return

            print(f"✓ DONE (json-lines): {output_path.name} → {total_rows} rows")

        except Exception:
            buffer.clear()
            batch_index = 0
            total_rows = 0

        # =========================
        # TRY 2: JSON LINES / MONGO
        # =========================
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = sanitize_mongo_json_line(line)
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except Exception:
                    continue

                if not isinstance(item, dict):
                    continue

                row = {
                    k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                    for k, v in item.items()
                }
                buffer.append(row)

                if len(buffer) >= chunk_size:
                    flush_buffer()

        flush_buffer()
        print(f"✓ DONE (json-lines): {output_path.name} → {total_rows} rows")

    except Exception as e:
        print(f"❌ ERROR processing {file_path.name}: {e}")

def parse_tsv_fallback(file_path: Path, chunk_size: int = 10_000):
    print("⚠️  Fallback to TSV mode")

    output_path = OUTPUT_DIR / f"{file_path.stem}.csv"
    batch_index = 0
    total_rows = 0
    buffer = []

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split("\t")

            row = {f"col_{i+1}": v if v else "__NA__" for i, v in enumerate(parts)}
            buffer.append(row)

            if len(buffer) >= chunk_size:
                df = pd.DataFrame(buffer).fillna("__NA__")
                df.to_csv(
                    output_path,
                    mode="w" if batch_index == 0 else "a",
                    header=(batch_index == 0),
                    index=False,
                    encoding="utf-8-sig"
                )
                total_rows += len(df)
                batch_index += 1
                buffer.clear()
                print(f"  ✓ TSV batch {batch_index} ({total_rows} rows)")

    if buffer:
        df = pd.DataFrame(buffer).fillna("__NA__")
        df.to_csv(
            output_path,
            mode="w" if batch_index == 0 else "a",
            header=(batch_index == 0),
            index=False,
            encoding="utf-8-sig"
        )
        total_rows += len(df)

    print(f"✓ DONE (TSV): {output_path.name} → {total_rows} rows")

def run():
    json_files = list(INPUT_DIR.rglob("*.json"))

    print("===================================")
    print("STEP 1.2 – STREAMING JSON → CSV")
    print("===================================")
    print(f"Input dir : {INPUT_DIR}")
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"Found {len(json_files)} JSON files")
    print("-----------------------------------")

    if not json_files:
        print("⚠️  No JSON files found.")
        return

    for file_path in json_files:
        process_json_file(file_path)

    print("-----------------------------------")
    print("✓ STEP 1.2 COMPLETED")


if __name__ == "__main__":
    run()