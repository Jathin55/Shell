-- Databricks notebook source
CREATE TABLE IF NOT EXISTS jathin.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) location 'dbfs:/mnt/adlsshelldatabricks/raw/delta/jathin/people10m'

-- COMMAND ----------


