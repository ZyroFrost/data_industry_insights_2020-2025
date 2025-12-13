import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# ============================================================
# 1. Generate Companies (2,000 rows)
# ============================================================
def generate_companies(n=2000):
    industries = ["Finance", "E-commerce", "Technology", "Retail", "Telecom", "Healthcare"]
    sizes = ["Small (1-50)", "Medium (51-300)", "Large (300-1000)", "Enterprise (1000+)"]

    companies = []
    for i in range(1, n+1):
        companies.append({
            "company_id": i,
            "company_name": fake.company(),
            "size": random.choice(sizes),
            "founded_year": random.randint(1990, 2023),
            "industry": random.choice(industries),
            "headquarters": fake.city(),
            "website": f"https://{fake.domain_name()}",
            "revenue": random.randint(1_000_000, 500_000_000),
            "rating": round(random.uniform(2.5, 5.0), 2),
            "hr_contact": fake.email(),
            "last_active": fake.date_between(start_date="-1y", end_date="today")
        })

    return pd.DataFrame(companies)

# ============================================================
# 2. Generate Skills (2,000 rows)
# ============================================================
def generate_skills(n=2000):
    skill_list = ["Python", "SQL", "Spark", "AWS", "GCP", "Azure", "PowerBI",
                  "Tableau", "Machine Learning", "Deep Learning", "NLP",
                  "Data Modeling", "Hadoop", "Docker", "Kubernetes"]

    categories = ["Programming", "Data Engineering", "Visualization",
                  "Cloud", "Machine Learning", "Statistics"]

    skills = []
    for i in range(1, n+1):
        skills.append({
            "skill_id": i,
            "skill_name": random.choice(skill_list),
            "category": random.choice(categories),
            "difficulty_level": random.choice(["Easy", "Medium", "Hard"]),
            "popularity_score": random.randint(1, 100),
            "required_frequency": random.randint(1, 500),
            "certification": fake.word() + " Certificate",
            "avg_salary_effect": random.randint(0, 500),
            "created_at": fake.date_between(start_date="-5y", end_date="-1y"),
            "updated_at": fake.date_between(start_date="-1y", end_date="today"),
        })
    return pd.DataFrame(skills)

# ============================================================
# 3. Generate Locations (2,000 rows)
# ============================================================
def generate_locations(n=2000):
    cities = ["HCMC", "Hanoi", "Da Nang", "Can Tho", "Hai Phong"]

    locations = []
    for i in range(1, n+1):
        city = random.choice(cities)
        locations.append({
            "location_id": i,
            "city": city,
            "district": fake.word().capitalize(),
            "region": "South" if city == "HCMC" else ("North" if city in ["Hanoi", "Hai Phong"] else "Central"),
            "latitude": round(random.uniform(10, 22), 6),
            "longitude": round(random.uniform(105, 110), 6),
            "cost_index": random.randint(60, 140),
            "avg_salary_level": random.randint(800, 3000),
            "population": random.randint(100_000, 10_000_000),
            "remote_friendly": random.choice([True, False])
        })

    return pd.DataFrame(locations)

# ============================================================
# 4. Applicants_Stats (2,000 rows)
# ============================================================
def generate_applicants_stats(n=2000, skill_count=2000):
    stats = []
    for i in range(1, n+1):
        stats.append({
            "stat_id": i,
            "year": random.randint(2020, 2025),
            "skill_id": random.randint(1, skill_count),
            "applicant_count": random.randint(50, 5000),
            "avg_skill_score": round(random.uniform(1.0, 5.0), 2),
            "training_hours": random.randint(5, 200),
            "demand_index": random.randint(10, 100),
            "supply_index": random.randint(10, 100),
            "skill_gap": random.randint(-50, 50),
            "growth_rate": round(random.uniform(-0.3, 0.5), 2),
        })
    return pd.DataFrame(stats)

# ============================================================
# 5. MAIN DATASET: Job_Postings (200,000 rows)
# ============================================================
def generate_job_postings(n=400000, company_count=2000, skill_count=2000, location_count=2000):
    titles = ["Data Analyst", "Data Engineer", "Data Scientist", "BI Analyst",
              "Machine Learning Engineer", "AI Engineer", "Statistician"]
    levels = ["Junior", "Mid", "Senior", "Lead"]
    employment = ["Full-time", "Part-time", "Internship"]

    jobs = []
    for i in range(1, n+1):
        posted = fake.date_between(start_date="-5y", end_date="today")
        jobs.append({
            "job_id": i,
            "company_id": random.randint(1, company_count),
            "title": random.choice(titles),
            "level": random.choice(levels),
            "department": random.choice(["Data", "Engineering", "BI", "IT"]),
            "employment_type": random.choice(employment),
            "location_id": random.randint(1, location_count),
            "posted_date": posted,
            "expired_date": posted + timedelta(days=random.randint(7, 60)),
            "min_salary": random.randint(500, 2000),
            "max_salary": random.randint(2000, 6000),
            "currency": random.choice(["VND", "USD"]),
            "required_exp_years": random.randint(0, 10),
            "education_level": random.choice(["Bachelor", "Master", "PhD"]),
            "job_description": fake.sentence(nb_words=20),
            "skill_id": random.randint(1, skill_count),
            "job_status": random.choice(["Active", "Closed"]),
            "remote_option": random.choice([True, False]),
            "views": random.randint(10, 50000),
            "applications": random.randint(1, 1500),
            "industry": random.choice(["Data", "Finance", "Technology", "E-commerce"]),
            "dataset_generated_at": datetime.now()
        })
    return pd.DataFrame(jobs)

# ============================================================
# Generate all datasets
# ============================================================

print("Generating Companies...")
df_companies = generate_companies()

print("Generating Skills...")
df_skills = generate_skills()

print("Generating Locations...")
df_locations = generate_locations()

print("Generating Applicants Stats...")
df_stats = generate_applicants_stats()

print("Generating Job Postings (200,000 rows)...")
df_jobs = generate_job_postings()

# ============================================================
# Save CSV files
# ============================================================
df_companies.to_csv("companies.csv", index=False)
df_skills.to_csv("skills.csv", index=False)
df_locations.to_csv("locations.csv", index=False)
df_stats.to_csv("applicants_stats.csv", index=False)
df_jobs.to_csv("job_postings_200k.csv", index=False)

print("DONE! Dataset đã được tạo đầy đủ.")