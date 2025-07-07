"""
AWS Glue Job - Electric Vehicle Data Processing.
Processes JSON data from S3, generates Parquet and uploads to Snowflake.

This script implements a complete ETL pipeline that:
1. Reads electric vehicle JSON data from S3
2. Processes and transforms the data using PySpark
3. Saves full backup in Parquet format
4. Select and type 25 specific columns
5. Load final data into Snowflake table.
"""

# Import system libraries
import sys                                

# Import specific libraries from AWS Glue
from awsglue.transforms import *              
from awsglue.utils import getResolvedOptions 
from pyspark.context import SparkContext     
from awsglue.context import GlueContext     
from awsglue.job import Job                   

# Importing our customized library with ETL functions
from custom_library import EVETLUtils

# Extract parameters passed to the job from the AWS Glue Console

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',              # Job name for logging and tracking
    'SOURCE_S3_BUCKET',      # S3 Bucket where the source JSON files are located
    'SOURCE_S3_KEY',         # Specific path to the JSON file within the bucket
    'TARGET_S3_BUCKET',      # Bucket S3 where the Parquet files will be stored
    'SNOWFLAKE_SECRET_NAME'  # Name of the secret in AWS Secrets Manager
])

# Initialize Spark and Glue contexts required for processing
sc = SparkContext()                    
glueContext = GlueContext(sc)         
spark = glueContext.spark_session     
job = Job(glueContext)                 
job.init(args['JOB_NAME'], args)      

def main():

    # CloudWatch Logs tracking startup message
    print("Start AWS Glue Job - Processing of electric vehicles")
    
    # ========== STEP 1: DATA EXTRACTION ==========
    source_path = f"s3://{args['SOURCE_S3_BUCKET']}/{args['SOURCE_S3_KEY']}"
    print(f"Reading JSON from {source_path}")
    
    # Using our custom function to read JSON from S3
    json_data = EVETLUtils.read_json_from_s3(
        args['SOURCE_S3_BUCKET'],  
        args['SOURCE_S3_KEY']
    )
    
    # ========== STEP 2: STRUCTURE ANALYSIS ==========
    column_names = EVETLUtils.extract_column_names(json_data)
    
    # Extract all rows of data from the 'data' section of the JSON
    data_rows = EVETLUtils.extract_data_rows(json_data)
    
    # Log of basic metrics for monitoring
    print(f"Extracted {len(column_names)} columns and {len(data_rows)} rows")
    
    # ========== STEP 3: CREATION OF RAW DATAFRAME ==========
    schema = EVETLUtils.create_string_schema(column_names)
    
    # Create Spark DataFrame combining data and schema
    df = spark.createDataFrame(data_rows, schema)
    
    # Confirm successful creation of the DataFrame (this executes an action in Spark)
    print(f"DataFrame created with {df.count()} rows")
    
    # ========== STEP 4: RAW STORAGE IN PARQUET ==========
    parquet_path = f"s3://{args['TARGET_S3_BUCKET']}/electric_vehicles_raw/"
    print(f"Typing Parquet raw a {parquet_path}")
    
    # Write complete DataFrame to S3 in Parquet format
    df.write \
        .mode("overwrite") \
        .parquet(parquet_path)
    
    # ========== STEP 5: TRANSFORMATION TO SNOWFLAKE ==========
    print("Reading Parquet and transforming for Snowflake...")
    
    # Read the newly created parquet (this ensures consistency)
    df_parquet = spark.read.parquet(parquet_path)
    
    # Apply specific transformations for Snowflake:
    df_snowflake = EVETLUtils.select_and_transform_columns(df_parquet)
    
    # ========== STEP 6: UPLOAD TO SNOWFLAKE==========
    print("Cargando datos a Snowflake...")
    
    # Get Snowflake's credentials from AWS Secrets Manager
    snowflake_creds = EVETLUtils.get_snowflake_credentials(args['SNOWFLAKE_SECRET_NAME'])
    
    # Write DataFrame transformed directly to Snowflake table
    EVETLUtils.write_to_snowflake(df_snowflake, snowflake_creds)
    
    # ========== FINAL SUMMARY AND METRICS ==========
    print("Job successfully completed!")
    print("=" * 50)
    print("PROCESSING METRICS:")
    print(f"- Processed rows: {df_snowflake.count():,}")           
    print(f"- Total columns in JSON: {len(column_names)}")        
    print(f"- Columns charged to Snowflake: {len(df_snowflake.columns)}") 
    print(f"- Parquet raw stored in: {parquet_path}")              
    print(f"- Data loaded in Snowflake: ELECTRIC_VEHICLES_WA")   
    print("=" * 50)

# Script entry point
if __name__ == "__main__":
    main()
    job.commit()