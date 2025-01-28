CREATE DATABASE dev;

CREATE SCHEMA dev.raw_data;
CREATE SCHEMA dev.analytics;
CREATE SCHEMA dev.adhoc;

CREATE OR REPLACE TABLE dev.raw_data.nps (
    id int PRIMARY KEY,
    created timestamp,
    score smallint
);

SELECT * FROM dev.raw_data.nps LIMIT 10;

CREATE TABLE dev.analytics.monthly_nps AS
SELECT 
    LEFT(created, 7) month,
    AVG(score) nps
FROM dev.raw_data.nps
GROUP BY 1;

SELECT *
FROM dev.analytics.monthly_nps
ORDER BY month
LIMIT 10;
