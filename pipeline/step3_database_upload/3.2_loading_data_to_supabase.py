import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import os
from typing import List, Dict
from datetime import datetime

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "data_processed"

DB_CONFIG = {
    "host": os.getenv("DB_SUPABASE_HOST"),
    "port": os.getenv("DB_SUPABASE_PORT"),
    "dbname": os.getenv("DB_SUPABASE_NAME"),
    "user": os.getenv("DB_SUPABASE_USER"),
    "password": os.getenv("DB_SUPABASE_PASS"),
}

BATCH_SIZE = 5000

TABLE_MAPPINGS = {
    "companies.csv": {
        "table": "companies",
        "id_column": "company_id",
        "columns": ["company_name", "size", "industry"]
    },
    "locations.csv": {
        "table": "locations",
        "id_column": "location_id",
        "columns": ["city", "country", "country_iso", "latitude", "longitude", "population"]
    },
    "role_names.csv": {
        "table": "role_names",
        "id_column": "role_id",
        "columns": ["role_name"]
    },
    "skills.csv": {
        "table": "skills",
        "id_column": "skill_id",
        "columns": ["skill_name", "skill_category"]
    },
    "job_postings.csv": {
        "table": "job_postings",
        "id_column": "job_id",
        "columns": ["company_id", "location_id", "posted_date", "min_salary", 
                   "max_salary", "currency", "required_exp_years", "education_level", 
                   "employment_type", "remote_option", "job_description"]
    },
    "job_roles.csv": {
        "table": "job_roles",
        "id_column": None,
        "columns": ["job_id", "role_id"]
    },
    "job_levels.csv": {
        "table": "job_levels",
        "id_column": None,
        "columns": ["job_id", "level"]
    },
    "job_skills.csv": {
        "table": "job_skills",
        "id_column": None,
        "columns": ["job_id", "skill_id"]
    }
}

ID_MAPPING = {
    "company_id": {},
    "location_id": {},
    "role_id": {},
    "skill_id": {},
    "job_id": {}
}

TABLE_STATS = {
    "companies": {"inserted": 0, "skipped": 0},
    "locations": {"inserted": 0, "skipped": 0},
    "role_names": {"inserted": 0, "skipped": 0},
    "skills": {"inserted": 0, "skipped": 0},
    "job_postings": {"inserted": 0, "skipped": 0},
    "job_roles": {"inserted": 0, "skipped": 0},
    "job_levels": {"inserted": 0, "skipped": 0},
    "job_skills": {"inserted": 0, "skipped": 0},
}

def log_progress(message, prefix="ℹ️"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {prefix} {message}", flush=True)

def clean_value(value, column_name=""):
    if pd.isna(value):
        return None
    
    if isinstance(value, str):
        stripped = value.strip()
        if stripped in ["__NA__", "__INVALID__", "__UNMATCHED__", '_NA_', 'NA', 'nan', 'NaN', '']:
            return None
        if column_name == "population":
            try:
                return int(float(stripped))
            except:
                return None
        return stripped

    if isinstance(value, (np.integer, np.floating)):
        if np.isnan(value):
            return None
        if isinstance(value, np.integer):
            return int(value)
        float_val = float(value)
        if column_name in ['population', 'required_exp_years']:
            return int(float_val)
        return float_val
    
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    
    return value

def get_db_connection():
    try:
        log_progress("Đang kết nối database...")
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=30)
        conn.autocommit = False
        cur = conn.cursor()
        conn.commit()
        cur.close()
        log_progress("Kết nối database thành công", "✓")
        return conn
    except Exception as e:
        log_progress(f"Lỗi kết nối: {e}", "❌")
        return None

def get_table_row_count(conn, table_name: str) -> int:
    """Đếm số rows hiện tại trong bảng"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        cursor.close()
        return 0

def check_all_tables_status(conn) -> Dict[str, int]:
    """Check status của tất cả các bảng"""
    log_progress("Đang kiểm tra database hiện tại...")
    
    all_tables = [
        "companies", "locations", "role_names", "skills",
        "job_postings", "job_roles", "job_levels", "job_skills"
    ]
    
    table_counts = {}
    for table in all_tables:
        count = get_table_row_count(conn, table)
        table_counts[table] = count
    
    return table_counts

def load_existing_id_mapping(conn, id_column: str, table_name: str):
    """Load ID mapping từ DB (nếu bảng đã có data và không xóa)"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT {id_column} FROM {table_name}")
        rows = cursor.fetchall()
        cursor.close()
        
        for row in rows:
            new_id = row[0]
            ID_MAPPING[id_column][new_id] = new_id
        
        log_progress(f"Đã load {len(rows):,} {id_column} mapping từ DB", "✓")
    except Exception as e:
        cursor.close()
        log_progress(f"Không thể load mapping: {e}", "⚠️")

def batch_insert_with_returning(conn, table_name, columns, data_batch, id_column):
    """Batch insert với RETURNING"""
    if not data_batch:
        return []
    
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    
    values_list = []
    flat_values = []
    for row in data_batch:
        values_list.append(f"({placeholders})")
        flat_values.extend(row)
    
    query = f"INSERT INTO {table_name} ({cols_str}) VALUES {', '.join(values_list)} RETURNING {id_column}"
    
    cursor = conn.cursor()
    try:
        cursor.execute(query, flat_values)
        new_ids = [row[0] for row in cursor.fetchall()]
        conn.commit()
        cursor.close()
        return new_ids
    except Exception as e:
        conn.rollback()
        cursor.close()
        raise e

def batch_insert_no_returning(conn, table_name, columns, data_batch):
    """Batch insert không RETURNING"""
    if not data_batch:
        return
    
    cols_str = ", ".join(columns)
    cursor = conn.cursor()
    try:
        query = f"INSERT INTO {table_name} ({cols_str}) VALUES %s"
        psycopg2.extras.execute_values(cursor, query, data_batch, page_size=BATCH_SIZE)
        conn.commit()
        cursor.close()
    except Exception as e:
        conn.rollback()
        cursor.close()
        raise e

def validate_fk_exists(fk_value, fk_column, allow_null=False):
    """Kiểm tra FK có tồn tại không"""
    if fk_value is None:
        return allow_null
    
    if fk_column in ID_MAPPING:
        return fk_value in ID_MAPPING[fk_column]
    
    return True

def get_csv_row_count(file_path: Path) -> int:
    """Đếm số rows trong CSV file"""
    try:
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
        return len(df)
    except Exception as e:
        log_progress(f"Không đọc được {file_path.name}: {e}", "⚠️")
        return 0

def load_csv_to_db_optimized(conn, csv_file: str, table_name: str, id_column: str, 
                             columns: List[str], skip: bool = False):
    """Load CSV với option truncate"""
    if skip:
        log_progress(f"Bỏ qua {csv_file} (marked as skip)", "⏭️")
        return True
    
    file_path = DATA_DIR / csv_file
    if not file_path.exists():
        log_progress(f"File không tồn tại: {csv_file}", "⚠️")
        return False
    
    # Check DB hiện tại
    current_count = get_table_row_count(conn, table_name)
    csv_count = get_csv_row_count(file_path)
    
    # So sánh và hiển thị
    print(f"\n📋 Bảng: {table_name}")
    print(f"   DB hiện tại: {current_count:,}/{csv_count:,} rows", end="")
    
    if current_count == 0:
        print(" 🆕 (trống)")
    elif current_count < csv_count:
        missing = csv_count - current_count
        print(f" ⚠️  (thiếu {missing:,} rows)")
    elif current_count == csv_count:
        print(" ✓ (đầy đủ)")
    elif current_count > csv_count:
        extra = current_count - csv_count
        print(f" ❓ (thừa {extra:,} rows)")
    
    choice = input(f"   Xóa và load lại? (y/n): ").lower().strip()
    
    if choice != 'y':
        log_progress(f"Skip {table_name} - giữ nguyên data hiện tại", "⏭️")
        
        # Load ID mapping từ DB để FK vẫn hoạt động
        if id_column and current_count > 0:
            load_existing_id_mapping(conn, id_column, table_name)
        
        return True
    
    # Truncate bảng
    log_progress(f"Xóa dữ liệu bảng {table_name}...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
        conn.commit()
        cursor.close()
        log_progress(f"Đã xóa {table_name}", "✓")
    except Exception as e:
        conn.rollback()
        cursor.close()
        log_progress(f"Lỗi truncate: {e}", "❌")
        return False
    
    # Tiếp tục load CSV
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    log_progress(f"Đọc {csv_file} ({file_size_mb:.1f}MB)...")
    
    try:
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
        df.columns = df.columns.str.strip()
        total_rows = len(df)
        
        log_progress(f"Đã đọc {total_rows:,} rows, bắt đầu xử lý...")
        
        if df.empty:
            return True
        
        data_batch = []
        old_ids = []
        inserted_count = 0
        skipped_count = 0
        last_pct = 0
        
        for idx, row in df.iterrows():
            pct = int((idx / total_rows) * 100)
            if pct >= last_pct + 10:
                log_progress(f"  {pct}% ({idx:,}/{total_rows:,}) | inserted: {inserted_count:,} | skipped: {skipped_count:,}")
                last_pct = pct
            
            try:
                old_id = int(row[id_column]) if id_column and id_column in df.columns else None
                values = [clean_value(row[col], col) for col in columns]
                
                # Validation
                if table_name == "companies" and values[0] is None:
                    skipped_count += 1
                    continue
                
                if table_name == "role_names" and values[0] is None:
                    skipped_count += 1
                    continue
                
                if table_name == "job_postings":
                    for i, col in enumerate(columns):
                        if col in ["min_salary", "max_salary", "required_exp_years", "posted_date"]:
                            if values[i] in ["__NA__", "__INVALID__", "__UNMATCHED__"]:
                                values[i] = None
                    
                    company_id = values[0]
                    location_id = values[1]
                    
                    if company_id is None or location_id is None:
                        skipped_count += 1
                        continue
                    
                    if not validate_fk_exists(company_id, "company_id"):
                        if skipped_count < 10:
                            log_progress(f"Row {idx}: company_id={company_id} không tồn tại", "⚠️")
                        skipped_count += 1
                        continue
                    
                    if not validate_fk_exists(location_id, "location_id"):
                        if skipped_count < 10:
                            log_progress(f"Row {idx}: location_id={location_id} không tồn tại", "⚠️")
                        skipped_count += 1
                        continue
                    
                    if len(values) > 8 and values[8] not in ["Full-time", "Part-time", "Internship", "Temporary", None]:
                        values[8] = None
                    
                    if len(values) > 9 and values[9] in [None]:
                        values[9] = None
                
                if table_name == "companies":
                    if values[1] not in ["Startup", "Small", "Medium", "Large", "Enterprise", None]:
                        values[1] = None
                    if values[2] not in ["Technology", "Finance", "Banking", "Insurance",
                        "Healthcare", "Education", "E-commerce", "Manufacturing",
                        "Consulting", "Government", "Telecommunications", "Energy",
                        "Retail", "Logistics", "Real Estate", None]:
                        values[2] = None
                
                data_batch.append(values)
                if old_id:
                    old_ids.append(old_id)
                
                # Insert batch
                if len(data_batch) >= BATCH_SIZE:
                    try:
                        if id_column:
                            new_ids = batch_insert_with_returning(conn, table_name, columns, data_batch, id_column)
                            for old, new in zip(old_ids, new_ids):
                                ID_MAPPING[id_column][old] = new
                        else:
                            batch_insert_no_returning(conn, table_name, columns, data_batch)
                        
                        inserted_count += len(data_batch)
                        data_batch = []
                        old_ids = []
                    except Exception as batch_error:
                        log_progress(f"Batch lỗi tại row ~{idx}: {str(batch_error)[:100]}", "❌")
                        skipped_count += len(data_batch)
                        data_batch = []
                        old_ids = []
                        continue
                
            except Exception as e:
                skipped_count += 1
                if skipped_count <= 5:
                    log_progress(f"Row {idx}: {str(e)[:80]}", "⚠️")
                continue
        
        # Insert batch cuối
        if data_batch:
            try:
                if id_column:
                    new_ids = batch_insert_with_returning(conn, table_name, columns, data_batch, id_column)
                    for old, new in zip(old_ids, new_ids):
                        ID_MAPPING[id_column][old] = new
                else:
                    batch_insert_no_returning(conn, table_name, columns, data_batch)
                inserted_count += len(data_batch)
            except Exception as batch_error:
                log_progress(f"Batch cuối lỗi: {str(batch_error)[:100]}", "❌")
                skipped_count += len(data_batch)
        
        TABLE_STATS[table_name]["inserted"] = inserted_count
        TABLE_STATS[table_name]["skipped"] = skipped_count
        
        log_progress(f"{table_name}: {inserted_count:,} inserted, {skipped_count:,} skipped", "✓")
        return True
        
    except Exception as e:
        conn.rollback()
        log_progress(f"Lỗi: {e}", "❌")
        import traceback
        traceback.print_exc()
        return False

def load_csv_with_fk_mapping_optimized(conn, csv_file: str, table_name: str, 
                                      columns: List[str], skip: bool = False):
    """Load junction tables"""
    if skip:
        log_progress(f"Bỏ qua {csv_file} (marked as skip)", "⏭️")
        return True
    
    file_path = DATA_DIR / csv_file
    if not file_path.exists():
        log_progress(f"File không tồn tại: {csv_file}", "⚠️")
        return False
    
    # Check DB hiện tại
    current_count = get_table_row_count(conn, table_name)
    csv_count = get_csv_row_count(file_path)
    
    # So sánh và hiển thị
    print(f"\n📋 Bảng: {table_name}")
    print(f"   DB hiện tại: {current_count:,}/{csv_count:,} rows", end="")
    
    if current_count == 0:
        print(" 🆕 (trống)")
    elif current_count < csv_count:
        missing = csv_count - current_count
        print(f" ⚠️  (thiếu {missing:,} rows)")
    elif current_count == csv_count:
        print(" ✓ (đầy đủ)")
    elif current_count > csv_count:
        extra = current_count - csv_count
        print(f" ❓ (thừa {extra:,} rows)")
    
    choice = input(f"   Xóa và load lại? (y/n): ").lower().strip()
    
    if choice != 'y':
        log_progress(f"Skip {table_name} - giữ nguyên data hiện tại", "⏭️")
        return True
    
    # Truncate
    log_progress(f"Xóa dữ liệu bảng {table_name}...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
        conn.commit()
        cursor.close()
        log_progress(f"Đã xóa {table_name}", "✓")
    except Exception as e:
        conn.rollback()
        cursor.close()
        log_progress(f"Lỗi truncate: {e}", "❌")
        return False
    
    # Tiếp tục load CSV
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    log_progress(f"Đọc {csv_file} ({file_size_mb:.1f}MB)...")
    
    try:
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
        df.columns = df.columns.str.strip()
        total_rows = len(df)
        
        log_progress(f"Đã đọc {total_rows:,} rows, bắt đầu xử lý...")
        
        if df.empty:
            return True
        
        data_batch = []
        inserted_count = 0
        skipped_count = 0
        last_pct = 0
        
        for idx, row in df.iterrows():
            pct = int((idx / total_rows) * 100)
            if pct >= last_pct + 10:
                log_progress(f"  {pct}% ({idx:,}/{total_rows:,}) | inserted: {inserted_count:,} | skipped: {skipped_count:,}")
                last_pct = pct
            
            try:
                values = []
                for col in columns:
                    val = clean_value(row[col], col)
                    if col == "level" and val is None:
                        val = "__NA__"
                    
                    if col == "job_id":
                        values.append(val)
                        continue
                    
                    if col in ID_MAPPING and val is not None:
                        if val not in ID_MAPPING[col]:
                            raise ValueError(f"FK {col}={val} không tồn tại")
                        val = ID_MAPPING[col][val]
                    
                    values.append(val)
                
                if any(v is None for v in values[:2]):
                    skipped_count += 1
                    continue
                
                data_batch.append(values)
                
                if len(data_batch) >= BATCH_SIZE:
                    try:
                        batch_insert_no_returning(conn, table_name, columns, data_batch)
                        inserted_count += len(data_batch)
                        data_batch = []
                    except Exception as batch_error:
                        log_progress(f"Batch lỗi tại row ~{idx}: {str(batch_error)[:100]}", "❌")
                        skipped_count += len(data_batch)
                        data_batch = []
                        continue
                
            except Exception as e:
                skipped_count += 1
                if skipped_count <= 5:
                    log_progress(f"Row {idx}: {str(e)[:80]}", "⚠️")
                continue
        
        if data_batch:
            try:
                batch_insert_no_returning(conn, table_name, columns, data_batch)
                inserted_count += len(data_batch)
            except Exception as batch_error:
                log_progress(f"Batch cuối lỗi: {str(batch_error)[:100]}", "❌")
                skipped_count += len(data_batch)
        
        TABLE_STATS[table_name]["inserted"] = inserted_count
        TABLE_STATS[table_name]["skipped"] = skipped_count
        
        log_progress(f"{table_name}: {inserted_count:,} inserted, {skipped_count:,} skipped", "✓")
        return True
        
    except Exception as e:
        conn.rollback()
        log_progress(f"Lỗi: {e}", "❌")
        return False

def main():
    print("=" * 70)
    print("🚀 UPLOAD DỮ LIỆU (SMART MODE)")
    print("=" * 70)
    
    conn = get_db_connection()
    if not conn:
        return
    
    log_progress(f"Data directory: {DATA_DIR}")
    log_progress(f"Batch size: {BATCH_SIZE:,} rows/batch")
    
    # Check tất cả bảng trước
    table_counts = check_all_tables_status(conn)
    
    print("\n" + "=" * 70)
    print("📊 DATABASE HIỆN TẠI")
    print("=" * 70)
    for table, count in table_counts.items():
        print(f"{table:<20} | {count:>10,} rows")
    
    print("\n" + "=" * 70)
    print("💡 Bạn sẽ được hỏi từng bảng trước khi load")
    print("   Y = Xóa và load lại | N = Giữ nguyên data cũ")
    print("=" * 70)
    
    input("\nNhấn Enter để tiếp tục...")
    
    start_time = datetime.now()
    success_count = 0
    
    # PHASE 1
    print("\n" + "=" * 70)
    print("📦 PHASE 1: Parent tables")
    print("=" * 70)
    parent_tables = ["companies.csv", "locations.csv", "role_names.csv", "skills.csv"]
    for csv_file in parent_tables:
        if csv_file in TABLE_MAPPINGS:
            config = TABLE_MAPPINGS[csv_file]
            if load_csv_to_db_optimized(conn, csv_file, config["table"], 
                             config["id_column"], config["columns"], 
                             config.get("skip", False)):
                success_count += 1
    
    # PHASE 2
    print("\n" + "=" * 70)
    print("📦 PHASE 2: Job postings")
    print("=" * 70)
    config = TABLE_MAPPINGS["job_postings.csv"]
    if load_csv_to_db_optimized(conn, "job_postings.csv", config["table"], 
                     config["id_column"], config["columns"], 
                     config.get("skip", False)):
        success_count += 1
    
    # PHASE 3
    print("\n" + "=" * 70)
    print("📦 PHASE 3: Junction tables")
    print("=" * 70)
    junction_tables = ["job_roles.csv", "job_levels.csv", "job_skills.csv"]
    for csv_file in junction_tables:
        if csv_file in TABLE_MAPPINGS:
            config = TABLE_MAPPINGS[csv_file]
            if load_csv_with_fk_mapping_optimized(conn, csv_file, config["table"], 
                                       config["columns"], config.get("skip", False)):
                success_count += 1
    
    conn.close()
    
    elapsed = datetime.now() - start_time
    
    print("\n" + "=" * 70)
    print(f"✅ HOÀN THÀNH")
    print(f"⏱️  Tổng thời gian: {elapsed}")
    print("=" * 70)
    
    print("\n📊 LOAD SUMMARY")
    print("-" * 70)
    for table, stat in TABLE_STATS.items():
        if stat['inserted'] > 0 or stat['skipped'] > 0:
            print(f"{table:<20} | inserted: {stat['inserted']:>10,} | skipped: {stat['skipped']:>8,}")
    
    print("\n📊 ID MAPPING")
    print("-" * 70)
    for id_col, mapping in ID_MAPPING.items():
        if mapping:
            print(f"{id_col:<20} | {len(mapping):>10,} mappings")
    
    total_inserted = sum(s['inserted'] for s in TABLE_STATS.values())
    if elapsed.total_seconds() > 0:
        rows_per_sec = total_inserted / elapsed.total_seconds()
        print(f"\n⚡ Throughput: {rows_per_sec:,.0f} rows/second")

if __name__ == "__main__":
    main()