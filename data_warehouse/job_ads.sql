-- ============================
-- 🦆 DuckDB Setup Script
-- For: Jobtech Analysis Project
-- ============================

-- 1️⃣ Create main database (DuckDB handles this automatically when you connect)
-- Example in Python:
-- import duckdb
-- con = duckdb.connect("jobtech_analysis.duckdb")

-- 2️⃣ Create schemas (to organize data)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS public;

-- 3️⃣ (Optional) Show schemas
SHOW SCHEMAS;

-- 4️⃣ Example: Create sample tables inside each schema

-- In staging: raw input data
CREATE TABLE IF NOT EXISTS staging.job_ads_raw AS
SELECT * FROM read_csv_auto('data/job_ads.csv');

-- In warehouse: cleaned/processed data
CREATE TABLE IF NOT EXISTS warehouse.job_ads_cleaned AS
SELECT
    id,
    title,
    company,
    location,
    salary,
    posted_date
FROM staging.job_ads_raw
WHERE title IS NOT NULL;

-- In marts: ready-to-use analytics tables
CREATE TABLE IF NOT EXISTS marts.job_ads_summary AS
SELECT
    location,
    COUNT(*) AS total_jobs,
    AVG(salary) AS avg_salary
FROM warehouse.job_ads_cleaned
GROUP BY location;

-- 5️⃣ Example query
SELECT * FROM marts.job_ads_summary
ORDER BY total_jobs DESC;

-- 6️⃣ (Optional) Show all tables by schema
SHOW TABLES FROM staging;
SHOW TABLES FROM warehouse;
SHOW TABLES FROM marts;
