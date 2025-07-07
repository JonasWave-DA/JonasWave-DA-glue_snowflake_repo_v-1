-- =====================================================
-- SNOWFLAKE: WASHINGTON STATE ELECTRIC VEHICLES
-- Dataset: Electric and Hybrid Vehicle Registrations
-- Source: Washington State Department of Licensing
-- =====================================================

CREATE OR REPLACE TABLE ELECTRIC_VEHICLES_WA (
    SID                                      VARCHAR(50)          -- sid,
    ,ID                                       VARCHAR(50)         -- id,
    ,POSITION                                 NUMBER(10,0)         -- position,
    ,CREATED_AT                               TIMESTAMP_NTZ        -- created_at,
    ,CREATED_META                             VARCHAR(100)         -- created_meta,
    ,UPDATED_AT                               TIMESTAMP_NTZ        -- updated_at,
    ,VIN_1_10                                 VARCHAR(10)          -- VIN (1-10),
    ,COUNTY                                   VARCHAR(50)          -- County,
    ,CITY                                     VARCHAR(100)         -- City,
    ,STATE                                    VARCHAR(2)           -- State,
    ,POSTAL_CODE                              VARCHAR(10)          -- Postal Code,
    ,MODEL_YEAR                               NUMBER(4,0)          -- Model Year,
    ,MAKE                                     VARCHAR(50)          -- Make,
    ,MODEL                                    VARCHAR(100)         -- Model,
    ,ELECTRIC_VEHICLE_TYPE                    VARCHAR(100)         -- Electric Vehicle Type,
    ,CLEAN_ALTERNATIVE_FUEL_VEHICLE_CAFV_ELIGIBILITY VARCHAR(200)         -- Clean Alternative Fuel Vehicle (CAFV) Eligibility,
    ,ELECTRIC_RANGE                           NUMBER(10,0)         -- Electric Range,
    ,BASE_MSRP                                NUMBER(12,2)         -- Base MSRP,
    ,LEGISLATIVE_DISTRICT                     NUMBER(3,0)          -- Legislative District,
    ,DOL_VEHICLE_ID                           VARCHAR(50)          -- DOL Vehicle ID,
    ,VEHICLE_LOCATION                         GEOGRAPHY            -- Vehicle Location,
    ,ELECTRIC_UTILITY                         VARCHAR(500)         -- Electric Utility,
    ,"2020_CENSUS_TRACT"                        VARCHAR(20)          -- 2020 Census Tract,
    ,COUNTIES                                 NUMBER(10,0)         -- Counties,
    ,CONGRESSIONAL_DISTRICTS                  NUMBER(3,0)          -- Congressional Districts,
    ,WAOFM___GIS___LEGISLATIVE_DISTRICT_BOUNDARY NUMBER(10,0)         -- WAOFM - GIS - Legislative District Boundary
)
COMMENT = 'Electric vehicle population data for the state of Washington'
;

-- =====================================================
-- RECOMMENDED INDEXES AND OPTIMIZATIONS
-- =====================================================

-- Clustering key to optimize queries by location
ALTER TABLE ELECTRIC_VEHICLES_WA CLUSTER BY (STATE, COUNTY, CITY);

-- =====================================================
-- SAMPLE QUERIES FOR ANALYSIS
-- =====================================================

-- Vehicles by city (most popular for registration)
-- SELECT CITY, COUNT(*) as VEHICLE_COUNT 
-- FROM ELECTRIC_VEHICLES_WA 
-- WHERE STATE = 'WA' 
-- GROUP BY CITY 
-- ORDER BY VEHICLE_COUNT DESC;

-- Distribution by type of electric vehicle
-- SELECT ELECTRIC_VEHICLE_TYPE, COUNT(*) as COUNT
-- FROM ELECTRIC_VEHICLES_WA 
-- GROUP BY ELECTRIC_VEHICLE_TYPE;

-- Trend by model year
-- SELECT MODEL_YEAR, COUNT(*) as REGISTRATIONS
-- FROM ELECTRIC_VEHICLES_WA 
-- WHERE MODEL_YEAR IS NOT NULL
-- GROUP BY MODEL_YEAR 
-- ORDER BY MODEL_YEAR;
