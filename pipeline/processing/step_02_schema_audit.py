from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "data_processing"
OUTPUT_FILE = ROOT / "data" / "schema_audit.xlsx"


def run_schema_audit(data_dir: Path, sample_size=3):
    records = []

    for csv_file in data_dir.glob("*.csv"):
        source = csv_file.stem  # dùng làm tên cột pivot

        try:
            df = pd.read_csv(csv_file, nrows=300)
        except Exception:
            continue

        for col in df.columns:
            samples = (
                df[col]
                .dropna()
                .astype(str)
                .unique()[:sample_size]
            )

            records.append({
                "column_name": col.strip().lower(),
                "source": source,
                "sample_values": " | ".join(samples)
            })

    audit_df = pd.DataFrame(records)

    # ===== PIVOT =====
    pivot_df = audit_df.pivot_table(
        index="column_name",
        columns="source",
        values="sample_values",
        aggfunc="first"
    ).reset_index()

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        audit_df.to_excel(writer, sheet_name="raw_schema", index=False)
        pivot_df.to_excel(writer, sheet_name="pivot_schema", index=False)

    print(f"Schema audit exported to: {OUTPUT_FILE}")
    return pivot_df


if __name__ == "__main__":
    run_schema_audit(DATA_DIR)