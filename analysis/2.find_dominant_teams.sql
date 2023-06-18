-- Databricks notebook source
SELECT
  team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING total_races >= 50
ORDER BY total_points DESC;

-- COMMAND ----------

