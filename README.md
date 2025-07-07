# Electric Vehicle Data ETL Pipeline

AWS Glue job for processing electric vehicle data from JSON to Parquet format and loading into Snowflake with dynamic column mapping and validation.

## Overview

This project processes electric vehicle registration data from Washington State, transforming JSON data stored in S3 into Parquet format and loading it into Snowflake for analytics. Features dynamic column parsing and strict validation to prevent unauthorized schema changes.

## System Architecture

- **Source**: JSON files in S3
- **Processing**: AWS Glue with PySpark + Snowpark
- **Storage**: Raw Parquet files in S3 + Transformed data
- **Configuration**: External JSON configuration in S3
- **Destination**: Snowflake data warehouse

## Project Structure

```
glue_snowflake_repo/
├── electric-vehicle-processing/
│   └── aws_glue_job.py          # Main Glue job script
├── custom_library/
│   ├── __init__.py              # Python package initializer
│   └── ev_etl_utils.py          # Reusable ETL utilities
├── ElectricVehiclePopulationData.json  # Sample data
├── snowflake_table_structure.sql       # Snowflake table structure
├── snowpark_parquet_loader.py           # Snowpark script to load Parquet from S3
└── ev-etl-utils.zip             # Packaged custom library
```

## Detailed Code Explanation

### 1. Main AWS Glue Script (aws_glue_job.py)

This file contains the main logic of the AWS Glue job using **PySpark**:

**Lines 1-11**: Library imports
- `sys`: For system arguments
- `awsglue.*`: AWS Glue specific libraries for transformations
- `pyspark.*`: Distributed processing engine
- `custom_library`: Our custom library with ETL functions

**Lines 13-19**: Parameter configuration
- `getResolvedOptions()`: Extracts parameters passed to the job from Glue console
- Required parameters: S3 buckets, file paths, Snowflake credentials

**Lines 21-26**: Context initialization
- `SparkContext`: Base Spark context for distributed processing
- `GlueContext`: Glue-specific context with additional functionalities
- `Job`: Object for job lifecycle management

### 2. Snowpark Script (snowpark_loader.py)

This file uses **Snowpark** to load data directly into Snowflake:

**get_credentials() function** (lines 9-18):
- Connects to AWS Secrets Manager using boto3
- Gets Snowflake credentials from configured secret
- Parses JSON and returns dictionary with credentials

**create_session() function** (lines 20-32):
- Creates Snowpark session using obtained credentials
- Configures connection parameters (account, user, password, warehouse, etc.)
- Returns active session ready to execute commands

**create_stage() function** (lines 40-52):
- Creates external stage in Snowflake pointing to S3 bucket
- Gets AWS credentials from another secret for S3 access
- Executes DDL command using `session.sql()`

**create_table() function** (lines 54-78):
- Creates destination table with optimized 25-column schema
- Defines appropriate data types for each field
- Configures clustering key for efficient geographic queries

**load_data() function** (lines 108-170):
- **Key process**: Extracts embedded JSON from Parquet files
- Uses `PARSE_JSON($1)` to extract individual fields from JSON
- Maps each JSON field to table column with type conversion
- Executes `COPY INTO` with real-time transformation

## Key Features

- **Hybrid Processing**: AWS Glue (PySpark) + Snowpark
- **Dual Pipeline**: JSON → Parquet (Glue) → Snowflake (Snowpark)
- **JSON Extraction**: Handling embedded JSON in Parquet
- **Secure Management**: AWS Secrets Manager for credentials
- **Optimization**: Clustering keys and appropriate data types

## Loading Parquet from S3 to Snowflake

### Option 1: Snowpark Script (snowpark_loader.py)

**Key features:**
- **Snowpark Connection**: `Session.builder.configs().create()`
- **External Stage**: Direct S3 access from Snowflake
- **JSON Transformation**: `PARSE_JSON($1)` to extract fields
- **Optimized Load**: `COPY INTO` with column mapping

**Processing flow:**
```python
# 1. Connect to Snowflake
session = Session.builder.configs(params).create()

# 2. Create stage with AWS credentials
CREATE OR REPLACE STAGE stage_name
URL = 's3://bucket/path/'
CREDENTIALS = (AWS_KEY_ID = '...', AWS_SECRET_KEY = '...')

# 3. Extract JSON and load
COPY INTO table_name (col1, col2, ...)
FROM (
    SELECT 
        PARSE_JSON($1):field1::VARCHAR as col1,
        PARSE_JSON($1):field2::NUMBER as col2
    FROM @stage_name
)
```

**Execution:**
```bash
python snowpark_loader.py
```

### Option 2: SQL Script (load_parquet_from_s3.sql)

SQL script executable directly in Snowflake Worksheet with native commands.

## Required Configuration

### Secrets in AWS Secrets Manager

**1. Snowflake Credentials** (`snowflake-connection-credentials`):
```json
{
  "SNOWFLAKE_URL": "account.snowflakecomputing.com",
  "SNOWFLAKE_USER": "username",
  "SNOWFLAKE_PASSWORD": "password",
  "SNOWFLAKE_DATABASE": "database",
  "SNOWFLAKE_SCHEMA": "schema",
  "SNOWFLAKE_WAREHOUSE": "warehouse"
}
```

**2. AWS S3 Credentials** (`snowpark-access`):
```json
{
  "AWS_ACCESS_KEY_ID": "AKIA...",
  "AWS_SECRET_ACCESS_KEY": "secret..."
}
```

### Required IAM Permissions

**IAM User for Snowflake:**
- `s3:GetObject` on specific bucket
- `s3:ListBucket` on specific bucket
- S3FullAccess policy or custom policy

## Complete Data Flow

### AWS Glue Pipeline (Step 1)
```
JSON (S3) → PySpark → Raw Parquet (S3) → Snowflake (main table)
```

### Snowpark Pipeline (Step 2)
```
Raw Parquet (S3) → Snowpark → JSON Extraction → New Snowflake Table
```

## Hybrid Approach Advantages

**AWS Glue (PySpark):**
- Distributed processing for large volumes
- Complex data transformations
- Complete backup in Parquet

**Snowpark:**
- Native processing in Snowflake
- Lower latency (no data transfer)
- Efficient embedded JSON extraction
- SQL + Python in same environment

## Use Cases

**Use AWS Glue when:**
- Need complex transformations
- Processing multiple data sources
- Require complete backup in Parquet

**Use Snowpark when:**
- Data already close to Snowflake
- Need fast and direct processing
- Working primarily with SQL + Python

## Performance Metrics

**AWS Glue Job:**
- ~150,000 rows processed
- ~30 columns → 25 selected columns
- Time: 5-10 minutes
- Output: Parquet (~50MB) + Snowflake table

**Snowpark Loader:**
- ~22,000 rows loaded (example)
- Embedded JSON → 25 structured columns
- Time: 1-2 minutes
- Output: New table `ELECTRIC_VEHICLES_WA_SNOW`

## Best Practices

### Security
- **Never hardcode credentials** in code
- Use **AWS Secrets Manager** for all credentials
- **Principle of least privilege** for IAM roles
- **Encryption** enabled in S3 and Snowflake

### Performance
- **Clustering keys** in Snowflake tables for geographic queries
- **Partitioning** of Parquet files by date/region
- **Appropriate warehouse sizing** in Snowflake
- **DataFrame caching** when possible

### Monitoring
- **CloudWatch Logs** for AWS Glue
- **Query History** in Snowflake for Snowpark
- **Custom metrics** for tracking processed rows
- **Alerts** for failures or excessive time

## Common Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `AccessDenied` S3 | Insufficient IAM permissions | Check user's S3 policies |
| `SecretNotFound` | Secret doesn't exist | Create secrets in Secrets Manager |
| `SnowflakeConnectorError` | Snowflake credentials | Verify secret JSON format |
| `PARSE_JSON` error | Malformed JSON | Inspect Parquet structure |
| `Session timeout` | Lost connection | Verify network connectivity |

## Recommended Analysis Queries

### On main table (AWS Glue)
```sql
SELECT MAKE, AVG(ELECTRIC_RANGE) as avg_range
FROM ELECTRIC_VEHICLES_WA 
WHERE ELECTRIC_RANGE > 0
GROUP BY MAKE ORDER BY avg_range DESC;
```

### On Snowpark table
```sql
SELECT ELECTRIC_VEHICLE_TYPE, COUNT(*) as total
FROM ELECTRIC_VEHICLES_WA_SNOW 
GROUP BY ELECTRIC_VEHICLE_TYPE;
```

## Improvement Roadmap

- [x] **AWS Glue Pipeline**: JSON → Parquet → Snowflake
- [x] **Snowpark Pipeline**: Parquet → Snowflake with JSON extraction
- [ ] **Incremental Processing**: Only new/modified records
- [ ] **Quality Validation**: Automatic checks
- [ ] **Orchestration**: Step Functions
- [ ] **Advanced Monitoring**: Real-time dashboard# glue_snowflake_repo_v1
