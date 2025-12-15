# Data Industry Insights
## Overview
End-to-end data pipeline and analytics project analyzing global Data job market trends (2020–2025), with web crawlers, cleaned datasets, Power BI dashboard, and Streamlit insights app.

## 📁 Project Folder Structure
```
data_industry_insights/
│
├── app/                                    # Streamlit UI / Giao diện Streamlit
│   └── app.py
│
├── analysis/                               # Manual data checks & notes / Kiểm tra dữ liệu thủ công
│   ├── schema_mapping_notes.md
│   └── schema_audit.xlsx
│
├── dashboard/                              # Power BI dashboard
│   └── Data_Industry_Insights.pbix
│
├── database/                               # Database schema & ERD (chỉ chứa cấu trúc tạo bảng SQL và mô hình ERD)
│   ├── schema.sql                          # SQL schema / Tạo bảng database
│   ├── ERD.png                             # Entity Relationship Di
│   └── README.md                           # Database structure and usage notes / Giải thích cấu trúc và cách dùng database
│
├── data/                                   # DATA FILES ONLY / FOLDER CHỈ CHỨA DATA (JSON VÀ CSV SAU KHI LẤY TỪ PIPELINE)
│   ├── data_raw/                           # Raw scraped data (API / HTML / JSON) / Dữ liệu thô (file JSON lấy trực tiếp từ web)
│   └── data_processing/                    # Transformed intermediate data / Dữ liệu chuyển đổi (file CSV sau khi parse từ JSON)
│   │   ├── data_extracted/                 # Extracted raw fields / Dữ liệu trích xuất trực tiếp từ JSON
│   │   ├── data_mapped/                    # Mapped & standardized data / Dữ liệu đã map và chuẩn hóa cột
│   │   └── data_enriched/                  # After augmentation & derivation) / Dữ liệu đã được làm giàu (bổ sung, suy diễn thêm thuộc tính)
│   ├── data_processed/                     # Cleaned final data for analytics / Dữ liệu cuối để phân tích (đã merge và tách bảng)
│   ├── data_seeds/                         # Lookup & reference data / Dữ liệu chuẩn tra cứu (không dùng cho pipeline chính)
│   └── metadata/                           # Schema & source documentation / Tài liệu mô tả cấu trúc JSON của từng nguồn web
│
├── pipeline/                               # DATA PIPELINE LOGIC / LOGIC XỬ LÝ DỮ LIỆU (FOLDER CHỈ CHỨA CODE PYTHON)
│   ├── crawlers/
│   │   ├── api/                            # Crawl via APIs
│   │   │   ├── authenticated/              # APIs require key / API cần đăng ký
│   │   │   └── public/                     # Public APIs / API công khai
│   │   └── scrape/                         # HTML web scraping
│   │       ├── protected/                  # Anti-bot sites (testing only) / Web có chống bot
│   │       └── public/                     # Public websites / Web công khai
│   │
│   ├── processing/                         # Cleaning & normalization logic / Làm sạch dữ liệu
│   │
│   ├── seeds/                              # Fake data for testing only / Dữ liệu giả để test
│   │   └── seed_data.py
│   │
│   ├── tools/                              # Helper tools for data processing / Công cụ hỗ trợ chạy thủ công
│   │   └── column_mapper_app.py            # Column mapping and normalization tool / App hỗ trợ map và kiểm tra tên cột
│   │
│   └── main.py                             # Pipeline entry point / File chạy chính
```
