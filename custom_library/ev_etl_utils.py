"""
Custom Library for AWS Glue - Electric Vehicle ETL Utils
Reusable functions for electric vehicle data processing

This library contains all specialized functions for:
- Reading JSON files from S3
- Extracting and processing data schemas
- Snowflake data type transformations
- Secure connectivity to AWS Secrets Manager
- Direct write to Snowflake using Spark connector

All functions are designed to be reusable and modular.
"""

# Importing standard Python libraries
import json                    
import boto3                   # AWS SDK for interfacing with services (S3, Secrets Manager)

# Import data types from PySpark to define schemas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

# Importing PySpark functions for column transformations
from pyspark.sql.functions import col, trim, from_unixtime

class EVETLUtils:
   
    @staticmethod
    def get_snowflake_credentials(secret_name):

        # Create AWS Secrets Manager client using IAM role credentials
        secrets_client = boto3.client('secretsmanager')
        
        try:
            # Request the value of the secret by name
            response = secrets_client.get_secret_value(SecretId=secret_name)
            
            # The secret is stored as a JSON string
            return json.loads(response['SecretString'])
            
        except Exception as e:
            # Log  error to debugging in CloudWatch
            print(f"Error obtaining Secrets Manager credentials: {e}")
            print(f"Verify that the secret '{secret_name}' exists and is in valid JSON format")
            raise  

    @staticmethod
    def read_json_from_s3(bucket, key):

        # Create S3 client using Glue IAM role credentials
        s3_client = boto3.client('s3')
        
        # Download object from S3 (this returns metadata + content)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Read file contents
        json_content = response['Body'].read().decode('utf-8')
        
        # Parse JSON string to Python data structure
        return json.loads(json_content)

    @staticmethod
    def extract_column_names(json_data):

        # Navigate JSON structure to get to column definition
        columns = json_data['meta']['view']['columns']
        
        # Extract only the 'name' field from each column definition
        return [col['name'] for col in columns]

    @staticmethod
    def extract_data_rows(json_data):

        # The 'data' section contains all rows as a list of lists.
        return json_data['data']

    @staticmethod
    def create_string_schema(column_names):

        # StructType defines the complete structure of the DataFrame
        return StructType([StructField(name, StringType(), True) for name in column_names])

    @staticmethod
    def select_and_transform_columns(df):

        # ========== MAPPING JSON COLUMNS TO SNOWFLAKE ==========
        column_mapping = {
            # System metadata
            "sid": "SID",                                    
            "id": "ID",                                     
            "position": "POSITION",                         
            "created_at": "CREATED_AT",                     
            "updated_at": "UPDATED_AT",                     
            
            # Vehicle information
            "VIN (1-10)": "VIN_1_10",                       
            "Model Year": "MODEL_YEAR",                      
            "Make": "MAKE",                                  
            "Model": "MODEL",                                
            "Electric Vehicle Type": "ELECTRIC_VEHICLE_TYPE", 
            "Electric Range": "ELECTRIC_RANGE",              
            "Base MSRP": "BASE_MSRP",                        
            
            # Geographic location
            "County": "COUNTY",                             
            "City": "CITY",                                  
            "State": "STATE",                                
            "Postal Code": "POSTAL_CODE",                    
            "Vehicle Location": "VEHICLE_LOCATION",          
            
            # Regulatory and administrative information
            "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "CLEAN_ALTERNATIVE_FUEL_VEHICLE_CAFV_ELIGIBILITY",
            "Legislative District": "LEGISLATIVE_DISTRICT",   
            "DOL Vehicle ID": "DOL_VEHICLE_ID",              
            "Electric Utility": "ELECTRIC_UTILITY",          
            "2020 Census Tract": "2020_CENSUS_TRACT",        
            "Counties": "COUNTIES",                          
            "Congressional Districts": "CONGRESSIONAL_DISTRICTS", 
            "WAOFM - GIS - Legislative District Boundary": "WAOFM___GIS___LEGISLATIVE_DISTRICT_BOUNDARY"  
        }
        
        # ========== COLUMN RENAMING ==========
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:  # Verify column exists before renaming
                df = df.withColumnRenamed(old_name, new_name)
        
        # ========== COLUMN SELECTION ==========
        selected_columns = list(column_mapping.values())
        
        # Select only the columns that exist in the DataFrame
        df = df.select(*[col for col in selected_columns if col in df.columns])
        
        # ========== CONVERSIÓN DE TIPOS DE DATOS ==========        
        df = df.withColumn("POSITION", col("POSITION").cast(IntegerType())) \
               .withColumn("CREATED_AT", from_unixtime(col("CREATED_AT")).cast(TimestampType())) \
               .withColumn("UPDATED_AT", from_unixtime(col("UPDATED_AT")).cast(TimestampType())) \
               .withColumn("MODEL_YEAR", col("MODEL_YEAR").cast(IntegerType())) \
               .withColumn("ELECTRIC_RANGE", col("ELECTRIC_RANGE").cast(IntegerType())) \
               .withColumn("BASE_MSRP", col("BASE_MSRP").cast(DecimalType(12, 2))) \
               .withColumn("LEGISLATIVE_DISTRICT", col("LEGISLATIVE_DISTRICT").cast(IntegerType())) \
               .withColumn("COUNTIES", col("COUNTIES").cast(IntegerType())) \
               .withColumn("CONGRESSIONAL_DISTRICTS", col("CONGRESSIONAL_DISTRICTS").cast(IntegerType())) \
               .withColumn("WAOFM___GIS___LEGISLATIVE_DISTRICT_BOUNDARY", col("WAOFM___GIS___LEGISLATIVE_DISTRICT_BOUNDARY").cast(IntegerType()))
        
        # Explanation of conversions:
        # - IntegerType(): For integers (years, ranges, districts).
        # - TimestampType(): For dates and times
        # - DecimalType(12,2): For prices (12 digits total, 2 decimal places)
        # - from_unixtime(): Converts UNIX timestamp to readable date.

        # Values that can't be converted are automatically converted to NULL
        # Return transformed DataFrame ready for Snowflake
        return df

    @staticmethod
    def write_to_snowflake(df, snowflake_creds):
        
        # ========== CONFIGURACIÓN DE CONEXIÓN ==========
        snowflake_options = {
            "sfUrl": snowflake_creds['SNOWFLAKE_URL'],           # Snowflake account URL
            "sfUser": snowflake_creds['SNOWFLAKE_USER'],         # Snowflake User
            "sfPassword": snowflake_creds['SNOWFLAKE_PASSWORD'], # User's password
            "sfDatabase": snowflake_creds['SNOWFLAKE_DATABASE'], # Target database
            "sfSchema": snowflake_creds['SNOWFLAKE_SCHEMA'],     # Schema within the DB
            "sfWarehouse": snowflake_creds['SNOWFLAKE_WAREHOUSE'], # Warehouse for processing
            "dbtable": "ELECTRIC_VEHICLES_WA"                   # Target table name
        }
        
        # ========== WRITE TO SNOWFLAKE ==========
        df.write \
            .format("snowflake") \
            .options(**snowflake_options) \
            .mode("overwrite") \
            .save()
        
        # Parameter explanation:
        # - format("snowflake"): Uses the Spark-Snowflake connector.
        # - options(**snowflake_options): Passes all credentials
        # - mode("overwrite"): Replaces existing data in the table - If you want to add new data without deleting existing data, use mode("append").
        # - save(): Executes the write operation.
        #
        # The connector handles automatically:
        # - Creation of temporary files in S3
        # - COPY INTO command in Snowflake
        # - Cleanup of temporary files
        # - Connection error handling