 -- Example of UPSERT in Snowflake

-- Creating the target table
CREATE OR REPLACE TABLE target_table (
    id INT,
    name STRING,
    value INT
);

-- Creating the staging table with new data
CREATE OR REPLACE TABLE staging_table (
    id INT,
    name STRING,
    value INT
);

-- Inserting sample data into target table
INSERT INTO target_table (id, name, value)
VALUES 
    (1, 'Alice', 100),
    (2, 'Bob', 200);

-- Inserting new data into staging table (some new, some existing)
INSERT INTO staging_table (id, name, value)
VALUES 
    (1, 'Alice', 150), -- Existing record with updated value
    (3, 'Charlie', 300); -- New record

-- Performing the UPSERT operation
MERGE INTO target_table AS target
USING staging_table AS stage
ON target.id = stage.id
WHEN MATCHED THEN 
    UPDATE SET 
        target.name = stage.name,
        target.value = stage.value
WHEN NOT MATCHED THEN 
    INSERT (id, name, value)
    VALUES (stage.id, stage.name, stage.value);

SELECT *
FROM target_table;
