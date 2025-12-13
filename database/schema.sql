-- =========================================================
-- JOB MARKET DATA SCHEMA (PostgreSQL)
-- Time range: 2020–2025
-- Focus: Data / AI / Analytics roles
-- =========================================================

-- =========================
-- 1. SKILLS
-- =========================
CREATE TABLE skills (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(100) NOT NULL,
    certification_required BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT chk_skill_category CHECK (
        category IN (
            'Programming',
            'Data Engineering',
            'Machine Learning',
            'Cloud',
            'Visualization',
            'Database',
            'DevOps',
            'Analytics'
        )
    )
);

-- =========================
-- 2. SKILL ALIASES
-- =========================
CREATE TABLE skill_aliases (
    alias VARCHAR(100) PRIMARY KEY,
    skill_id INT NOT NULL,
    CONSTRAINT fk_skill_aliases_skill
        FOREIGN KEY (skill_id)
        REFERENCES skills(skill_id)
        ON DELETE CASCADE
);

-- =========================
-- 3. COMPANIES
-- =========================
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    size VARCHAR(50) NOT NULL,
    industry VARCHAR(100) NOT NULL,
    CONSTRAINT chk_company_size CHECK (
        size IN ('Startup', 'Small', 'Medium', 'Large', 'Enterprise')
    ),
    CONSTRAINT chk_company_industry CHECK (
        industry IN (
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
        )
    )
);

-- =========================
-- 4. LOCATIONS
-- =========================
CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    population INT
);

-- =========================
-- 5. ROLE NAMES
-- =========================
CREATE TABLE role_names (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(255) NOT NULL,
    level VARCHAR(20) NOT NULL,
    department VARCHAR(100) NOT NULL,
    employment_type VARCHAR(20) NOT NULL,
    CONSTRAINT chk_role_name CHECK (
        role_name IN (
            'Data Analyst',
            'Business Intelligence Analyst',
            'Data Engineer',
            'Analytics Engineer',
            'Data Scientist',
            'Machine Learning Engineer',
            'AI Engineer',
            'Data Architect',
            'BI Developer',
            'Data Manager'
        )
    ),
    CONSTRAINT chk_role_level CHECK (
        level IN ('Intern', 'Junior', 'Mid', 'Senior', 'Lead')
    ),
    CONSTRAINT chk_employment_type CHECK (
        employment_type IN (
            'Full-time',
            'Part-time',
            'Contract',
            'Internship',
            'Temporary'
        )
    )
);

-- =========================
-- 6. JOB POSTINGS
-- =========================
CREATE TABLE job_postings (
    job_id SERIAL PRIMARY KEY,
    role_id INT NOT NULL,
    company_id INT NOT NULL,
    location_id INT NOT NULL,
    posted_date DATE NOT NULL,
    expired_date DATE,
    min_salary DECIMAL(10, 2),
    max_salary DECIMAL(10, 2),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    required_exp_years INT,
    education_level VARCHAR(50) NOT NULL,
    job_description TEXT,
    remote_option BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT chk_education_level CHECK (
        education_level IN (
            'High School',
            'Bachelor',
            'Master',
            'PhD',
            'Unknown'
        )
    ),
    CONSTRAINT fk_job_role
        FOREIGN KEY (role_id)
        REFERENCES role_names(role_id),
    CONSTRAINT fk_job_company
        FOREIGN KEY (company_id)
        REFERENCES companies(company_id),
    CONSTRAINT fk_job_location
        FOREIGN KEY (location_id)
        REFERENCES locations(location_id)
);

-- =========================
-- 7. JOB SKILLS (M:N)
-- =========================
CREATE TABLE job_skills (
    job_id INT NOT NULL,
    skill_id INT NOT NULL,
    importance_level VARCHAR(20) NOT NULL,
    skill_level_required VARCHAR(20) NOT NULL,
    CONSTRAINT pk_job_skills PRIMARY KEY (job_id, skill_id),
    CONSTRAINT fk_job_skills_job
        FOREIGN KEY (job_id)
        REFERENCES job_postings(job_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_job_skills_skill
        FOREIGN KEY (skill_id)
        REFERENCES skills(skill_id)
        ON DELETE CASCADE,
    CONSTRAINT chk_importance_level CHECK (
        importance_level IN ('Required', 'Preferred', 'Nice-to-have')
    ),
    CONSTRAINT chk_skill_level_required CHECK (
        skill_level_required IN ('Basic', 'Intermediate', 'Advanced', 'Expert')
    )
);

-- =========================
-- 8. INDEXES (FOR BIG DATA)
-- =========================
CREATE INDEX idx_job_postings_posted_date ON job_postings(posted_date);
CREATE INDEX idx_job_postings_company_id ON job_postings(company_id);
CREATE INDEX idx_job_postings_location_id ON job_postings(location_id);

-- =========================
-- END OF SCHEMA
-- =========================