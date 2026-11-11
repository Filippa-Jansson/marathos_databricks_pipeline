CREATE OR REPLACE STREAMING TABLE marathos.gold.fct_results AS
SELECT
  result_id,
  event_id,
  athlete_id,
  athlete_age_category,
  athlete_performance,
  athlete_average_speed
FROM STREAM marathos.silver.marathos_obt;