CREATE DATABASE IF NOT EXISTS employee_db;
USE employee_db;

CREATE TABLE IF NOT EXISTS employees (
    employee_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(100),
    salary DOUBLE,
    joining_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS employee_staging (
    employee_id VARCHAR(50),
    name VARCHAR(100),
    department VARCHAR(100),
    salary DOUBLE,
    joining_date DATE,
    updated_at TIMESTAMP NULL,
    run_id VARCHAR(100),
    INDEX idx_run_id (run_id)
);

