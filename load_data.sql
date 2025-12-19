-- ============================================================================
-- Snowflake Loading Script for DaneSF CSV Files
-- Generated: 2025-12-19
-- 
-- INSTRUCTIONS:
-- 1. Update the S3_BUCKET_NAME and AWS credentials placeholders
-- 2. Create the storage integration first (requires ACCOUNTADMIN role)
-- 3. Run the DDL statements to create schema, stage, file format, and tables
-- 4. Run COPY INTO statements to load data from S3
-- ============================================================================


CREATE OR REPLACE DATABASE SGH;      -- << UPDATE THIS
USE WAREHOUSE COMPUTE_WH;            -- << UPDATE THIS
-- =============================================================================
-- CONFIGURATION - UPDATE THESE VALUES
-- =============================================================================

-- Set your database and schema context
USE DATABASE SGH;           -- << UPDATE THIS
USE SCHEMA PUBLIC;          -- << UPDATE THIS

-- Or create a new schema for this data
-- CREATE SCHEMA IF NOT EXISTS DANE_SF;
-- USE SCHEMA DANE_SF;


-- =============================================================================
-- STEP 1: CREATE STORAGE INTEGRATION (requires ACCOUNTADMIN role)
-- =============================================================================
-- This needs to be created once by an account admin


USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STAGE public_s3_stage
  URL = 's3://sgh-dane/';

-- =============================================================================
-- STEP 2: CREATE FILE FORMAT FOR CSV FILES
-- =============================================================================

CREATE OR REPLACE FILE FORMAT csv_format_dane
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  ENCODING = 'UTF8';

CREATE OR REPLACE FILE FORMAT csv_format_dane2
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'               -- CHANGED: Use semicolon
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  ENCODING = 'UTF8';           -- CHANGED: Fixes Polish characters (Ś, Ą, etc.)


-- =============================================================================
-- STEP 3: CREATE EXTERNAL STAGE POINTING TO S3
-- =============================================================================


-- Verify stage and list files (run after stage is created)
LIST @public_s3_stage;


-- =============================================================================
-- STEP 4: CREATE TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Table: BAZA_KLIENTOW
-- Source file: BAZA_KLIENTOW.csv (985 rows, 19 columns)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BAZA_KLIENTOW (
    PESEL               VARCHAR(20),
    IMIE                VARCHAR(100),
    NAZWISKO            VARCHAR(100),
    PLEC                VARCHAR(10),
    DATA_URODZENIA      DATE,
    MIASTO_ZAMELDOWANIA VARCHAR(200),
    ULICA_ZAMIESZKANIA  VARCHAR(200),
    NUMER_DOMU          VARCHAR(50),
    GRUPA               VARCHAR(50),
    ID_TARYFY           INTEGER,
    TEL_USED            INTEGER,
    TEL_SOLD            INTEGER,
    CZAS_W_MIESIACU     DECIMAL(10,2),
    CZAS_DZISIAJ        DECIMAL(10,2),
    STATUS              VARCHAR(10),
    LICZBA_PUNKTOW      INTEGER,
    PESEL_RC            INTEGER,
    PESEL_GENDER        VARCHAR(10),
    OSOBA               VARCHAR(200)
);


-- -----------------------------------------------------------------------------
-- Table: BAZA_KLIENTOW_WWW
-- Source file: BAZA_KLIENTOW_WWW.csv (985 rows, 10 columns)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BAZA_KLIENTOW_WWW (
    PESEL               VARCHAR(20),
    IMIE                VARCHAR(100),
    NAZWISKO            VARCHAR(100),
    PLEC                VARCHAR(10),
    DATA_URODZENIA      DATE,
    MIEJSCE_ZAMIESZKANIA VARCHAR(200),
    ULICA_ZAMIESZKANIA  VARCHAR(200),
    NUMER_DOMU          VARCHAR(50),
    KOD_POCZTOWY        VARCHAR(20),
    E_MAIL              VARCHAR(200)
);

CREATE OR REPLACE TABLE MIASTO_MC (
    NAZWA VARCHAR(100),
    NAZWA_MATCHCODE VARCHAR(50)
);



-- =============================================================================
-- STEP 5: COPY INTO - LOAD DATA FROM S3
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Load BAZA_KLIENTOW.csv
-- -----------------------------------------------------------------------------
COPY INTO BAZA_KLIENTOW
FROM @public_s3_stage/BAZA_KLIENTOW.csv
FILE_FORMAT = (FORMAT_NAME = csv_format_dane)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- -----------------------------------------------------------------------------
-- Load BAZA_KLIENTOW_WWW.csv
-- -----------------------------------------------------------------------------
COPY INTO BAZA_KLIENTOW_WWW
FROM @public_s3_stage/BAZA_KLIENTOW_WWW.csv
FILE_FORMAT = (FORMAT_NAME = csv_format_dane)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- -----------------------------------------------------------------------------
-- Load MIASTO_MC.csv
-- -----------------------------------------------------------------------------
COPY INTO MIASTO_MC
FROM @public_s3_stage/miasto_mc.csv
FILE_FORMAT = (FORMAT_NAME = csv_format_dane2)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- VALIDATE ERRORS:
-- https://docs.snowflake.com/en/sql-reference/functions/validate
-- SELECT * FROM TABLE(VALIDATE(MIASTO_MC, JOB_ID => '_last'));

-- =============================================================================
-- STEP 6: VALIDATION QUERIES - Run after loading
-- =============================================================================

-- Check row counts
SELECT 'BAZA_KLIENTOW' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BAZA_KLIENTOW
UNION ALL
SELECT 'BAZA_KLIENTOW_WWW', COUNT(*) FROM BAZA_KLIENTOW_WWW;

-- Sample data from each table
SELECT * FROM BAZA_KLIENTOW LIMIT 10;
SELECT * FROM BAZA_KLIENTOW_WWW LIMIT 10;
SELECT * FROM MIASTO_MC LIMIT 10;

CREATE OR REPLACE TABLE BAZA_KLIENTOW_UNION AS 
SELECT 
    PESEL, 
    IMIE, 
    NAZWISKO, 
    PLEC, 
    DATA_URODZENIA, 
    MIASTO_ZAMELDOWANIA, 
    ULICA_ZAMIESZKANIA, 
    NUMER_DOMU,
    -- Columns unique to BAZA_KLIENTOW (filled with NULL for WWW table)
    GRUPA, 
    ID_TARYFY, 
    TEL_USED, 
    TEL_SOLD, 
    CZAS_W_MIESIACU, 
    CZAS_DZISIAJ, 
    STATUS, 
    LICZBA_PUNKTOW, 
    PESEL_RC, 
    PESEL_GENDER, 
    OSOBA,
    -- Columns unique to BAZA_KLIENTOW_WWW (filled with NULL for main table)
    NULL AS KOD_POCZTOWY,
    NULL AS E_MAIL,
    'BAZA_KLIENTOW' AS SOURCE_SYSTEM -- Helpful for tracking record origin
FROM BAZA_KLIENTOW

UNION ALL

SELECT 
    PESEL, 
    IMIE, 
    NAZWISKO, 
    PLEC, 
    DATA_URODZENIA, 
    MIEJSCE_ZAMIESZKANIA, -- Aligned with MIASTO_ZAMELDOWANIA
    ULICA_ZAMIESZKANIA, 
    NUMER_DOMU,
    -- Filling missing BAZA_KLIENTOW columns with NULL
    NULL, -- GRUPA
    NULL, -- ID_TARYFY
    NULL, -- TEL_USED
    NULL, -- TEL_SOLD
    NULL, -- CZAS_W_MIESIACU
    NULL, -- CZAS_DZISIAJ
    NULL, -- STATUS
    NULL, -- LICZBA_PUNKTOW
    NULL, -- PESEL_RC
    NULL, -- PESEL_GENDER
    NULL, -- OSOBA
    -- Columns unique to BAZA_KLIENTOW_WWW
    KOD_POCZTOWY,
    E_MAIL,
    'BAZA_KLIENTOW_WWW' AS SOURCE_SYSTEM
FROM BAZA_KLIENTOW_WWW;

SELECT * FROM BAZA_KLIENTOW_UNION limit 10;


-- =============================================================================
-- CLEANUP COMMANDS (use with caution)
-- =============================================================================

/*
-- Drop tables
DROP TABLE IF EXISTS BAZA_KLIENTOW;
DROP TABLE IF EXISTS BAZA_KLIENTOW_WWW;

-- Drop stage
DROP STAGE IF EXISTS public_s3_stage;

-- Drop file format
DROP FILE FORMAT IF EXISTS csv_format_dane;

*/
