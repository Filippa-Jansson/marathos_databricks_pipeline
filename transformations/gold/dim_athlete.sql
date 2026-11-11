
CREATE OR REPLACE MATERIALIZED VIEW marathos.gold.dim_athlete AS
SELECT
  athlete_id,
  MAX_BY(athlete_country, event_date) AS athlete_country,
  MAX(athlete_year_of_birth) AS athlete_year_of_birth,
  MAX(athlete_gender) AS athlete_gender,
  MAX_BY(athlete_club, event_date) AS athlete_club
FROM marathos.silver.marathos_obt
GROUP BY athlete_id;