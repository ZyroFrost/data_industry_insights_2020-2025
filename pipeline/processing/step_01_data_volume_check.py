# This file count all csv files in the data_processing folder and print the result
# Read columns and testing merged data

import os
import pandas as pd
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "data_processing"

import pandas as pd

def count_csv_rows(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f) - 1  # trừ header


def audit_csv_basic(data_dir):
    records = []

    for csv_file in data_dir.glob("*.csv"):
        try:
            df_head = pd.read_csv(csv_file, nrows=0)

            records.append({
                "file_name": csv_file.name,
                "row_count": count_csv_rows(csv_file),
                "column_count": len(df_head.columns)
            })

        except Exception:
            records.append({
                "file_name": csv_file.name,
                "row_count": None,
                "column_count": None
            })

    df = pd.DataFrame(records)

    # ép kiểu số để tính tổng
    df["row_count"] = pd.to_numeric(df["row_count"], errors="coerce")
    df["column_count"] = pd.to_numeric(df["column_count"], errors="coerce")

    summary = {
        "total_files": len(df),
        "total_rows": int(df["row_count"].sum()),
        "total_columns": int(df["column_count"].sum()),
        "error_files": int(df["row_count"].isna().sum())
    }
    print(df)
    print(summary)

    return df, summary

if __name__ == "__main__":

    # Get all csv files in the data_processing folder
    audit_csv_basic(DATA_DIR)
    
    # with open(DATA_DIR/"DataScientist.csv", "r", encoding="utf-8", errors="ignore") as f:
    #     count = sum(1 for _ in f) - 1
    # print(count)

    # print("FILE:", __file__)
    # print("CWD :", Path.cwd())
    # print("DATA_DIR:", DATA_DIR)
    # print("CSV:", list(DATA_DIR.glob("*.csv")))