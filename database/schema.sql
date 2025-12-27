CREATE DATABASE data_industry_insights if not exists;

-- =========================
-- SKILLS
-- =========================
CREATE TABLE skills (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) NOT NULL UNIQUE,
    skill_category VARCHAR(100) NOT NULL
        CHECK (skill_category IN (
            'Programming',
            'Data Engineering',
            'Machine Learning',
            'Cloud',
            'Visualization',
            'Database',
            'DevOps',
            'Analytics'
        ))
);

-- =========================
-- COMPANIES
-- =========================
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    company_name TEXT NOT NULL,
    size VARCHAR(50)
        CHECK (size IN (
            'Startup',
            'Small',
            'Medium',
            'Large',
            'Enterprise'
        )),
    industry VARCHAR(100)
        CHECK (industry IN (
            'Technology',
            'Finance',
            'Banking',
            'Insurance',
            'Healthcare',
            'Education',
            'E-commerce',
            'Manufacturing',
            'Consulting',
            'Government',
            'Telecommunications',
            'Energy',
            'Retail',
            'Logistics',
            'Real Estate'
        ))
);

-- =========================
-- LOCATIONS
-- =========================
CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(100),
    country_iso CHAR(2),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    population INT,
    CONSTRAINT uq_locations UNIQUE (city, country, country_iso)
);

-- =========================
-- ROLE NAMES
-- =========================
CREATE TABLE role_names (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(255) NOT NULL UNIQUE
        CHECK (role_name IN (
            'Data Analyst',
            'Business Intelligence Analyst',
            'BI Developer',
            'Analytics Engineer',
            'Data Engineer',
            'Data Scientist',
            'Machine Learning Engineer',
            'AI Engineer',
            'AI Researcher',
            'Applied Scientist',
            'Research Engineer',
            'Data Architect',
            'Data Manager',
            'Data Lead'
        ))
);

-- =========================
-- JOB POSTINGS
-- =========================
CREATE TABLE job_postings (
    job_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES companies(company_id),
    location_id INT NOT NULL REFERENCES locations(location_id),

    posted_date DATE,

    min_salary DECIMAL(10,2),
    max_salary DECIMAL(10,2),
    currency CHAR(3),

    required_exp_years INT,

    education_level VARCHAR(50)
        CHECK (education_level IN (
            'High School',
            'Bachelor',
            'Master',
            'PhD'
        )),

    employment_type VARCHAR(20)
        CHECK (employment_type IN (
            'Full-time',
            'Part-time',
            'Internship',
            'Temporary'
        )),

    job_description TEXT,

    remote_option VARCHAR(20)
        CHECK (remote_option IN (
            'Onsite',
            'Hybrid',
            'Remote'
        ))
);

-- indexes cho job_postings
CREATE INDEX idx_job_postings_posted_date ON job_postings(posted_date);
CREATE INDEX idx_job_postings_company_id ON job_postings(company_id);
CREATE INDEX idx_job_postings_location_id ON job_postings(location_id);

-- =========================
-- JOB SKILLS (M:N)
-- =========================
CREATE TABLE job_skills (
    job_id INT NOT NULL REFERENCES job_postings(job_id),
    skill_id INT NOT NULL REFERENCES skills(skill_id),
    skill_level_required VARCHAR(20)
        CHECK (skill_level_required IN (
            'Basic',
            'Intermediate',
            'Advanced',
            'Expert'
        )),
    PRIMARY KEY (job_id, skill_id)
);

-- =========================
-- JOB ROLES (M:N)
-- =========================
CREATE TABLE job_roles (
    job_id INT NOT NULL REFERENCES job_postings(job_id),
    role_id INT NOT NULL REFERENCES role_names(role_id),
    PRIMARY KEY (job_id, role_id)
);

-- =========================
-- JOB LEVELS
-- =========================
CREATE TABLE job_levels (
    job_id INT NOT NULL REFERENCES job_postings(job_id),
    level VARCHAR(20)
        CHECK (level IN (
            'Intern',
            'Junior',
            'Mid',
            'Senior',
            'Lead'
        )),
    PRIMARY KEY (job_id, level)
);