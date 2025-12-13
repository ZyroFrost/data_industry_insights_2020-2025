# Data Industry Insights
## Overview
End-to-end data pipeline and analytics project analyzing global Data job market trends (2020–2025), with web crawlers, cleaned datasets, Power BI dashboard, and Streamlit insights app.

## 📁 Project Folder Structure
```
data_industry_insights_2020-2025/
│
├── app/                                # Streamlit application (UI)
│   └── app.py
│
├── dashboard/                           # Power BI dashboard
│   └── Data_Industry_Trends.pbix
│
├── data/
│   ├── data_raw/                        # Raw scraped API/HTML/JSON data
│   ├── data_processing/                 # Parsed/converted intermediate data (not cleaned)
│   ├── data_processed/                  # Final cleaned datasets ready for analytics
│   ├── data_seeds/                      # Lookup tables & enrichment datasets
│   └── metadata/                        # Schema documentation
│
├── pipeline/                            # Data pipeline: crawlers + processing
│   ├── crawlers/
│   │   ├── api/
│   │   │   ├── authenticated/
│   │   │   └── public/
│   │   │
│   │   └── scrape
│   │       ├── protected/
│   │       └── public/
│   │
│   ├── processing/
│   │
│   ├── seeds/
│   │   └── seed_data.py
│   │
│   └── main.py                           # Pipeline entry point
│
├── database/                             # Database schema & ERD
│   ├── schema.sql                        # SQL script to create tables
│   ├── erd.png                           # Entity Relationship Diagram
│   └── README.md                         # Explanation of data model
│
├── .streamlit/                           # Streamlit configuration (secrets, settings)
│   └── secrets.toml
│
├── requirements.txt                      # Dependencies for pipeline
├── requirements_app.txt                  # Dependencies for Streamlit app
├── README.md
├── .gitignore
└── .env
```
