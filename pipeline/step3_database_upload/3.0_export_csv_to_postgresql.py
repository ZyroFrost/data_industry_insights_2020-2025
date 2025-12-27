import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Dict

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "data_processed"
OUTPUT_DIR = BASE_DIR / "database"

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
        "columns": ["skill_name", "skill_category", "certification_required"],
        "skip": True
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
        "columns": ["job_id", "level"],
        "skip": True
    },
    "job_skills.csv": {
        "table": "job_skills",
        "id_column": None,
        "columns": ["job_id", "skill_id", "skill_level_required"],
        "skip": True
    }
}

ID_MAPPING = {
    "company_id": {},
    "location_id": {},
    "role_id": {},
    "skill_id": {},
    "job_id": {}
}

def log_progress(message, prefix="ℹ️"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {prefix} {message}", flush=True)

def clean_value(value, column_name=""):
    """Clean và format giá trị cho SQL"""
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

def escape_sql_string(value):
    """Escape string cho SQL"""
    if value is None:
        return "NULL"
    
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    
    if isinstance(value, (int, float)):
        return str(value)
    
    if isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        # Escape backslashes
        escaped = escaped.replace("\\", "\\\\")
        return f"'{escaped}'"
    
    return f"'{str(value)}'"

def generate_insert_statement(table_name: str, columns: List[str], values: List):
    """Tạo câu lệnh INSERT"""
    cols_str = ", ".join(columns)
    vals_str = ", ".join([escape_sql_string(v) for v in values])
    return f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str});\n"

def csv_to_sql(csv_file: str, table_name: str, id_column: str, 
               columns: List[str], skip: bool = False):
    """Convert CSV sang SQL script"""
    if skip:
        log_progress(f"Bỏ qua {csv_file} (marked as skip)", "⏭️")
        return None
    
    file_path = DATA_DIR / csv_file
    if not file_path.exists():
        log_progress(f"File không tồn tại: {csv_file}", "⚠️")
        return None
    
    log_progress(f"Đang xử lý {csv_file}...")
    
    try:
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
        df.columns = df.columns.str.strip()
        total_rows = len(df)
        
        log_progress(f"Đã đọc {total_rows:,} rows")
        
        if df.empty:
            return []
        
        sql_statements = []
        processed = 0
        skipped = 0
        
        # Header comment
        sql_statements.append(f"-- Table: {table_name}\n")
        sql_statements.append(f"-- Source: {csv_file}\n")
        sql_statements.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Truncate statement
        sql_statements.append(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;\n\n")
        
        for idx, row in df.iterrows():
            try:
                old_id = int(row[id_column]) if id_column and id_column in df.columns else None
                
                values = [clean_value(row[col], col) for col in columns]
                
                # Validation
                if table_name == "companies" and values[0] is None:
                    skipped += 1
                    continue
                
                if table_name == "role_names" and values[0] is None:
                    skipped += 1
                    continue
                
                if table_name == "job_postings":
                    company_id = values[0]
                    location_id = values[1]
                    
                    if company_id is None or location_id is None:
                        skipped += 1
                        continue
                    
                    # Map FK
                    if company_id in ID_MAPPING["company_id"]:
                        values[0] = ID_MAPPING["company_id"][company_id]
                    if location_id in ID_MAPPING["location_id"]:
                        values[1] = ID_MAPPING["location_id"][location_id]
                    
                    # Validate employment_type
                    if len(values) > 8 and values[8] not in ["Full-time", "Part-time", "Internship", "Temporary", None]:
                        values[8] = None
                
                if table_name == "companies":
                    if values[1] not in ["Startup", "Small", "Medium", "Large", "Enterprise", None]:
                        values[1] = None
                    if values[2] not in ["Technology", "Finance", "Banking", "Insurance",
                        "Healthcare", "Education", "E-commerce", "Manufacturing",
                        "Consulting", "Government", "Telecommunications", "Energy",
                        "Retail", "Logistics", "Real Estate", None]:
                        values[2] = None
                
                # Generate INSERT
                insert_sql = generate_insert_statement(table_name, columns, values)
                sql_statements.append(insert_sql)
                
                # Save ID mapping for FK references
                if id_column and old_id:
                    # Giả sử SERIAL auto-increment từ 1
                    new_id = processed + 1
                    ID_MAPPING[id_column][old_id] = new_id
                
                processed += 1
                
                if processed % 1000 == 0:
                    log_progress(f"  Đã xử lý {processed:,}/{total_rows:,} rows")
                
            except Exception as e:
                skipped += 1
                if skipped <= 5:
                    log_progress(f"Row {idx}: {str(e)[:80]}", "⚠️")
                continue
        
        log_progress(f"{table_name}: {processed:,} inserted, {skipped:,} skipped", "✓")
        return sql_statements
        
    except Exception as e:
        log_progress(f"Lỗi: {e}", "❌")
        return None

def csv_to_sql_junction(csv_file: str, table_name: str, 
                        columns: List[str], skip: bool = False):
    """Convert junction table CSV sang SQL"""
    if skip:
        log_progress(f"Bỏ qua {csv_file} (marked as skip)", "⏭️")
        return None
    
    file_path = DATA_DIR / csv_file
    if not file_path.exists():
        log_progress(f"File không tồn tại: {csv_file}", "⚠️")
        return None
    
    log_progress(f"Đang xử lý {csv_file}...")
    
    try:
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
        df.columns = df.columns.str.strip()
        total_rows = len(df)
        
        log_progress(f"Đã đọc {total_rows:,} rows")
        
        if df.empty:
            return []
        
        sql_statements = []
        processed = 0
        skipped = 0
        
        # Header
        sql_statements.append(f"-- Table: {table_name}\n")
        sql_statements.append(f"-- Source: {csv_file}\n")
        sql_statements.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        sql_statements.append(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;\n\n")
        
        for idx, row in df.iterrows():
            try:
                values = []
                for col in columns:
                    val = clean_value(row[col], col)
                    
                    if col == "level" and val is None:
                        val = "__NA__"
                    if col == "skill_level_required" and val is None:
                        val = "__NA__"
                    
                    # Map FK
                    if col in ID_MAPPING and val is not None:
                        if val in ID_MAPPING[col]:
                            val = ID_MAPPING[col][val]
                    
                    values.append(val)
                
                if any(v is None for v in values[:2]):
                    skipped += 1
                    continue
                
                insert_sql = generate_insert_statement(table_name, columns, values)
                sql_statements.append(insert_sql)
                
                processed += 1
                
                if processed % 1000 == 0:
                    log_progress(f"  Đã xử lý {processed:,}/{total_rows:,} rows")
                
            except Exception as e:
                skipped += 1
                if skipped <= 5:
                    log_progress(f"Row {idx}: {str(e)[:80]}", "⚠️")
                continue
        
        log_progress(f"{table_name}: {processed:,} inserted, {skipped:,} skipped", "✓")
        return sql_statements
        
    except Exception as e:
        log_progress(f"Lỗi: {e}", "❌")
        return None

def main():
    print("=" * 70)
    print("🚀 CONVERT CSV TO POSTGRESQL SCRIPT")
    print("=" * 70)
    
    # Tạo output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    log_progress(f"Data directory: {DATA_DIR}")
    log_progress(f"Output directory: {OUTPUT_DIR}")
    
    start_time = datetime.now()
    
    # Single master SQL file
    master_file = OUTPUT_DIR / f"complete_database_insert_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
    master_statements = []
    
    # Header
    master_statements.append("/*\n")
    master_statements.append(" * COMPLETE DATABASE INSERT SCRIPT\n")
    master_statements.append(f" * Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    master_statements.append(" * Schema: data_industry_insights\n")
    master_statements.append(" * \n")
    master_statements.append(" * This script contains ALL tables data in correct order:\n")
    master_statements.append(" * 1. Parent tables (companies, locations, role_names, skills)\n")
    master_statements.append(" * 2. Job postings\n")
    master_statements.append(" * 3. Junction tables (job_roles, job_levels, job_skills)\n")
    master_statements.append(" */\n\n")
    
    master_statements.append("SET search_path TO data_industry_insights, public;\n")
    master_statements.append("SET client_encoding = 'UTF8';\n\n")
    
    master_statements.append("BEGIN;\n\n")
    
    # PHASE 1: Parent tables
    print("\n" + "=" * 70)
    print("📦 PHASE 1: Parent tables")
    print("=" * 70)
    
    master_statements.append("-- ============================================\n")
    master_statements.append("-- PHASE 1: PARENT TABLES\n")
    master_statements.append("-- ============================================\n\n")
    
    parent_tables = ["companies.csv", "locations.csv", "role_names.csv", "skills.csv"]
    for csv_file in parent_tables:
        if csv_file in TABLE_MAPPINGS:
            config = TABLE_MAPPINGS[csv_file]
            statements = csv_to_sql(csv_file, config["table"], 
                                   config["id_column"], config["columns"], 
                                   config.get("skip", False))
            
            if statements:
                master_statements.extend(statements)
                master_statements.append("\n")
    
    # PHASE 2: Job postings
    print("\n" + "=" * 70)
    print("📦 PHASE 2: Job postings")
    print("=" * 70)
    
    master_statements.append("-- ============================================\n")
    master_statements.append("-- PHASE 2: JOB POSTINGS\n")
    master_statements.append("-- ============================================\n\n")
    
    config = TABLE_MAPPINGS["job_postings.csv"]
    statements = csv_to_sql("job_postings.csv", config["table"], 
                           config["id_column"], config["columns"], 
                           config.get("skip", False))
    
    if statements:
        master_statements.extend(statements)
        master_statements.append("\n")
    
    # PHASE 3: Junction tables
    print("\n" + "=" * 70)
    print("📦 PHASE 3: Junction tables")
    print("=" * 70)
    
    master_statements.append("-- ============================================\n")
    master_statements.append("-- PHASE 3: JUNCTION TABLES\n")
    master_statements.append("-- ============================================\n\n")
    
    junction_tables = ["job_roles.csv", "job_levels.csv", "job_skills.csv"]
    for csv_file in junction_tables:
        if csv_file in TABLE_MAPPINGS:
            config = TABLE_MAPPINGS[csv_file]
            statements = csv_to_sql_junction(csv_file, config["table"], 
                                            config["columns"], 
                                            config.get("skip", False))
            
            if statements:
                master_statements.extend(statements)
                master_statements.append("\n")
    
    # Footer
    master_statements.append("COMMIT;\n\n")
    master_statements.append("-- Script completed successfully\n")
    
    # Save single master file
    log_progress(f"Đang ghi file SQL...")
    with open(master_file, 'w', encoding='utf-8') as f:
        f.writelines(master_statements)
    
    file_size_mb = master_file.stat().st_size / (1024 * 1024)
    
    elapsed = datetime.now() - start_time
    
    print("\n" + "=" * 70)
    print(f"✅ HOÀN THÀNH")
    print(f"⏱️  Tổng thời gian: {elapsed}")
    print(f"📁 Output file: {master_file.name}")
    print(f"📊 File size: {file_size_mb:.2f} MB")
    print("=" * 70)
    
    print("\n💡 Để import vào PostgreSQL, chạy:")
    print(f"   psql -U your_user -d your_db -f {master_file}")
    print("\n   Hoặc:")
    print(f"   psql -U your_user -d your_db < {master_file}")

if __name__ == "__main__":
    main()