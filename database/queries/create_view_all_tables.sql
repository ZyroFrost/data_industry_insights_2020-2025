--SET search_path TO data_industry_insights, public;

CREATE VIEW merge_all_tables AS
SELECT 
    jp.job_id,
    c.company_name,
    c.industry,
    c.size AS company_size,
    l.city,
    l.country,
    rn.role_name,
    jl.level AS career_level,
    s.skill_name,
    js.skill_level_required,
    jp.posted_date,
    jp.min_salary,
    jp.max_salary,
    jp.currency,
    jp.required_exp_years,
    jp.education_level,
    jp.employment_type,
    jp.remote_option
FROM job_postings jp
-- Joining Companies and Locations
LEFT JOIN companies c ON jp.company_id = c.company_id
LEFT JOIN locations l ON jp.location_id = l.location_id

-- Joining Job Levels
LEFT JOIN job_levels jl ON jp.job_id = jl.job_id

-- Joining Roles (Bridge table: Job_Roles -> Role_Names)
LEFT JOIN job_roles jr ON jp.job_id = jr.job_id
LEFT JOIN role_Names rn ON jr.role_id = rn.role_id

-- Joining Skills (Bridge table: Job_Skills -> Skills)
LEFT JOIN job_skills js ON jp.job_id = js.job_id
LEFT JOIN skills s ON js.skill_id = s.skill_id;