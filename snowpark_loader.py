"""
Snowpark Script - Loading Parquet from S3 to Snowflake
"""

from snowflake.snowpark import Session # Snowpark main library to connect to Snowflake
import json
import boto3

def get_credentials(secret_name):
    print(f"Conectando a AWS Secrets Manager...")
    
    # Usar credenciales de AWS CLI
    client = boto3.client('secretsmanager', region_name='us-east-1')
    
    print(f"Obteniendo secreto: {secret_name}")
    response = client.get_secret_value(SecretId=secret_name)
    
    print("Parseando credenciales...")
    return json.loads(response['SecretString'])

def create_session(secret_name):
    creds = get_credentials(secret_name)
    
    params = {
        "account": creds['SNOWFLAKE_URL'].replace('.snowflakecomputing.com', ''),
        "user": creds['SNOWFLAKE_USER'],
        "password": creds['SNOWFLAKE_PASSWORD'],
        "warehouse": creds['SNOWFLAKE_WAREHOUSE'],
        "database": creds['SNOWFLAKE_DATABASE'],
        "schema": creds['SNOWFLAKE_SCHEMA']
    }
    
    return Session.builder.configs(params).create()

def get_aws_s3_credentials(secret_name):
    """Obtener credenciales AWS para S3 desde Secrets Manager"""
    client = boto3.client('secretsmanager', region_name='us-east-1')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def create_stage(session, stage_name, s3_path):
    aws_creds = get_aws_s3_credentials('snowpark-access')
    
    sql = f"""
    CREATE OR REPLACE STAGE {stage_name}
    URL = '{s3_path}'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_creds['AWS_ACCESS_KEY_ID']}'
        AWS_SECRET_KEY = '{aws_creds['AWS_SECRET_ACCESS_KEY']}'
    )
    FILE_FORMAT = (TYPE = PARQUET)
    """
    session.sql(sql).collect()
    print(f"Stage creado con credenciales desde Secrets Manager: {stage_name}")

def create_table(session, table_name):
    sql = f"""
    CREATE OR REPLACE TABLE {table_name} (
        SID VARCHAR(50),
        ID VARCHAR(50),
        POSITION NUMBER(10,0),
        CREATED_AT TIMESTAMP_NTZ,
        UPDATED_AT TIMESTAMP_NTZ,
        VIN_1_10 VARCHAR(10),
        COUNTY VARCHAR(50),
        CITY VARCHAR(100),
        STATE VARCHAR(2),
        POSTAL_CODE VARCHAR(10),
        MODEL_YEAR NUMBER(4,0),
        MAKE VARCHAR(50),
        MODEL VARCHAR(100),
        ELECTRIC_VEHICLE_TYPE VARCHAR(100),
        CLEAN_ALTERNATIVE_FUEL_VEHICLE_CAFV_ELIGIBILITY VARCHAR(200),
        ELECTRIC_RANGE NUMBER(10,0),
        BASE_MSRP NUMBER(12,2),
        LEGISLATIVE_DISTRICT NUMBER(3,0),
        DOL_VEHICLE_ID VARCHAR(50),
        VEHICLE_LOCATION GEOGRAPHY,
        ELECTRIC_UTILITY VARCHAR(500),
        "2020_CENSUS_TRACT" VARCHAR(20),
        COUNTIES NUMBER(10,0),
        CONGRESSIONAL_DISTRICTS NUMBER(3,0),
        WAOFM___GIS___LEGISLATIVE_DISTRICT_BOUNDARY NUMBER(10,0)
    )
    CLUSTER BY (STATE, COUNTY, CITY)
    """
    session.sql(sql).collect() # All session is snowpark.session which is an .sql and .collect is the Snowpark library's own.
    print(f"Tabla creada: {table_name}")

def inspect_parquet(session, stage_name):
    """Inspeccionar esquema del Parquet"""
    inspect_sql = f"""
    SELECT *
    FROM @{stage_name}
    LIMIT 1
    """
    
    result = session.sql(inspect_sql).collect()
    print("Columnas en Parquet:")
    for row in result:
        print(row)
    
    # View file metadata
    metadata_sql = f"""
    SELECT 
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        *
    FROM @{stage_name}
    LIMIT 5
    """
    
    print("\nPrimeras 5 filas con metadata:")
    result = session.sql(metadata_sql).collect()
    for row in result:
        print(row)

def load_data(session, stage_name, table_name):
    # The data is in JSON inside the $1 column.
    # We need to extract each JSON field individually
    
    copy_sql = f"""
    COPY INTO {table_name} (
        SID,
        ID,
        POSITION,
        CREATED_AT,
        UPDATED_AT,
        VIN_1_10,
        COUNTY,
        CITY,
        STATE,
        POSTAL_CODE,
        MODEL_YEAR,
        MAKE,
        MODEL,
        ELECTRIC_VEHICLE_TYPE,
        CLEAN_ALTERNATIVE_FUEL_VEHICLE_CAFV_ELIGIBILITY,
        ELECTRIC_RANGE,
        BASE_MSRP,
        LEGISLATIVE_DISTRICT,
        DOL_VEHICLE_ID,
        VEHICLE_LOCATION,
        ELECTRIC_UTILITY,
        "2020_CENSUS_TRACT",
        COUNTIES,
        CONGRESSIONAL_DISTRICTS,
        WAOFM___GIS___LEGISLATIVE_DISTRICT_BOUNDARY
    )
    FROM (
        SELECT 
            PARSE_JSON($1):sid::VARCHAR as SID,
            PARSE_JSON($1):id::VARCHAR as ID,
            PARSE_JSON($1):position::NUMBER as POSITION,
            TO_TIMESTAMP(PARSE_JSON($1):created_at::NUMBER) as CREATED_AT,
            TO_TIMESTAMP(PARSE_JSON($1):updated_at::NUMBER) as UPDATED_AT,
            PARSE_JSON($1):"VIN (1-10)"::VARCHAR as VIN_1_10,
            PARSE_JSON($1):County::VARCHAR as COUNTY,
            PARSE_JSON($1):City::VARCHAR as CITY,
            PARSE_JSON($1):State::VARCHAR as STATE,
            PARSE_JSON($1):"Postal Code"::VARCHAR as POSTAL_CODE,
            PARSE_JSON($1):"Model Year"::NUMBER as MODEL_YEAR,
            PARSE_JSON($1):Make::VARCHAR as MAKE,
            PARSE_JSON($1):Model::VARCHAR as MODEL,
            PARSE_JSON($1):"Electric Vehicle Type"::VARCHAR as ELECTRIC_VEHICLE_TYPE,
            PARSE_JSON($1):"Clean Alternative Fuel Vehicle (CAFV) Eligibility"::VARCHAR as CLEAN_ALTERNATIVE_FUEL_VEHICLE_CAFV_ELIGIBILITY,
            PARSE_JSON($1):"Electric Range"::NUMBER as ELECTRIC_RANGE,
            PARSE_JSON($1):"Base MSRP"::NUMBER as BASE_MSRP,
            PARSE_JSON($1):"Legislative District"::NUMBER as LEGISLATIVE_DISTRICT,
            PARSE_JSON($1):"DOL Vehicle ID"::VARCHAR as DOL_VEHICLE_ID,
            PARSE_JSON($1):"Vehicle Location"::VARCHAR as VEHICLE_LOCATION,
            PARSE_JSON($1):"Electric Utility"::VARCHAR as ELECTRIC_UTILITY,
            PARSE_JSON($1):"2020 Census Tract"::VARCHAR as "2020_CENSUS_TRACT",
            PARSE_JSON($1):Counties::NUMBER as COUNTIES,
            PARSE_JSON($1):"Congressional Districts"::NUMBER as CONGRESSIONAL_DISTRICTS,
            PARSE_JSON($1):"WAOFM - GIS - Legislative District Boundary"::NUMBER as WAOFM___GIS___LEGISLATIVE_DISTRICT_BOUNDARY
        FROM @{stage_name}
    )
    """
    
    result = session.sql(copy_sql).collect()
    print(f"Result COPY: {result}")
    
    count = session.table(table_name).count()
    print(f"Rows loaded: {count:,}")
    return count

def main():
    STAGE_NAME = "EV_PARQUET_STAGE"
    TABLE_NAME = "ELECTRIC_VEHICLES_WA_SNOW"
    S3_PATH = "s3://dol-us-east-1-863162827303-dev-stage/electric_vehicles_raw/"
    SECRET_NAME = "snowflake-connection-credentials"
    

    
    session = None
    try:
        print("Obtaining credentials...")
        creds = get_credentials(SECRET_NAME)
        print(f"Connecting to account: {creds['SNOWFLAKE_URL']}")
        
        session = create_session(SECRET_NAME)
        print("Successful connection")
        
        create_stage(session, STAGE_NAME, S3_PATH)
        create_table(session, TABLE_NAME)
        count = load_data(session, STAGE_NAME, TABLE_NAME)
        print(f"Completed: {count:,} rows in {TABLE_NAME}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if session:
            session.close()

if __name__ == "__main__":
    main()