# Database Schema – Data Industry Insights (2020–2025)

This database stores **cleaned, normalized, and analytics-ready job market data**
for Data, AI, and Analytics roles collected from multiple global sources
between **2020 and 2025**.

The schema is designed for **trend analysis, salary insights, skill demand analysis,
and geographic comparisons**.

---

## Design Principles

- All crawling, cleaning, normalization, and deduplication are handled in the **Python ETL pipeline**
- The database stores **only clean and validated data**
- Fully normalized relational model (3NF)
- Optimized for analytical queries (BI, dashboards, reporting)
- Schema acts as a **data contract** between ETL and consumers

---

## Schema Overview

### Fact Table
- **job_postings**  
  Central fact table containing job-level information such as role, company,
  location, salary, and posting dates.

### Dimension Tables
- **skills** – Canonical skill definitions  
- **skill_aliases** – Normalized skill name mappings from raw text  
- **companies** – Company master data  
- **locations** – Geographic information (city, country, coordinates)  
- **role_names** – Standardized job roles and seniority levels  

### Bridge Tables
- **job_skills** – Many-to-many relationship between jobs and skills

---

## Table Relationships

- `job_postings.company_id` → `companies.company_id`
- `job_postings.location_id` → `locations.location_id`
- `job_postings.role_id` → `role_names.role_id`
- `job_skills.job_id` → `job_postings.job_id`
- `job_skills.skill_id` → `skills.skill_id`

Refer to **erd.png** for the visual relationship diagram.

---

## Data Responsibility Boundaries

| Layer | Responsibility |
|----|----|
| Crawlers | Data collection from APIs / websites |
| ETL (Python) | Parsing, cleaning, normalization, deduplication |
| Database | Storage of clean, structured data |
| BI / App | Analytics, visualization, insights |

---

## Notes

- Skill names are standardized using the `skills` and `skill_aliases` tables
- Salary fields may be NULL when not provided by the source
- `remote_option = false` represents "not specified" rather than explicitly on-site

---

## Usage

The database is intended to be consumed by:
- Power BI dashboards
- Streamlit analytics application
- Ad-hoc SQL analysis

This schema is **read-only for analytics consumers** and should only be written to
by the ETL pipeline.

---

## Files

| File | Description |
|----|----|
| schema.sql | SQL script to create all tables and constraints |
| erd.png | Entity Relationship Diagram |
| README.md | Database documentation |

