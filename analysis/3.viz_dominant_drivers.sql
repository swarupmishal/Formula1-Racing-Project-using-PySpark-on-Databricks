-- Databricks notebook source
SELECT
  driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points,
  RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM f1_presentation.calculated_race_results
-- WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY total_points DESC;

-- COMMAND ----------

WITH dominant_drivers AS (
  SELECT
    driver_name,
    COUNT(1) AS total_races,
    SUM(calculated_points) AS total_points,
    AVG(calculated_points) AS avg_points,
    RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
  -- WHERE race_year BETWEEN 2011 AND 2020
  GROUP BY driver_name
  HAVING total_races >= 50
  ORDER BY total_points DESC
)
SELECT
  race_year,
  driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

