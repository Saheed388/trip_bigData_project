import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from dotenv import load_dotenv
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# ------------------------------
# 1. Download taxi data
# ------------------------------
def download_files(year=2019):
    """Download yellow taxi data CSV files for a given year."""
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    months = range(1, 13)

    for month in months:
        file_name = f"yellow_tripdata_{year}-{month:02d}.csv.gz"
        url = base_url + file_name

        try:
            print(f" Downloading {file_name} ...")
            with requests.get(url, stream=True, timeout=60) as r:
                if r.status_code == 200:
                    with open(file_name, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                            f.write(chunk)
                    print(f" Downloaded {file_name}")
                else:
                    print(f" Skipped {file_name} (not found)")
        except Exception as e:
            print(f" Error downloading {file_name}: {e}")

# ------------------------------
# 2. Process CSV -> Parquet
# ------------------------------
def process_to_parquet(year=2019):
    """Efficiently process taxi CSV files into Parquet with Spark."""
    spark = SparkSession.builder \
        .appName("YellowTaxiToParquet") \
        .config("spark.sql.files.maxPartitionBytes", "256MB") \
        .master("spark://23733ff5f9d2:7077") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()

    input_path = f"yellow_tripdata_{year}-*.csv.gz"

    print(" Reading CSV files...")

    # Define schema explicitly for performance (no inferSchema scan)
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True)
    ])

    df = spark.read.schema(schema).csv(input_path, header=True)

    # Repartition for parallelism and avoid small files problem
    df = df.repartition(400, "tpep_pickup_datetime")  # balance by time column

    parquet_path = f"yellow_tripdata_{year}_all.parquet"
    print(f"✍️ Writing Parquet to {parquet_path} ...")

    df.write.mode("overwrite") \
      .option("compression", "snappy") \
      .parquet(parquet_path)

    print(" Parquet file written successfully")
    spark.stop()
    return parquet_path

# ------------------------------
# 3. Transform Parquet data
# ------------------------------
def transform_taxi_data(input_path, year=2019):
    """Transform taxi data by cleaning and splitting datetime columns."""
    spark = SparkSession.builder \
        .master("spark://23733ff5f9d2:7077") \
        .appName("TaxiDataTransformation") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()

    print(f" Reading Parquet file: {input_path}")
    df = spark.read.parquet(input_path)

    print("Debug: Input DataFrame schema")
    df.printSchema()

    # Drop rows with invalid VendorID or nulls
    cleaned_df = df.filter(col("VendorID").isNotNull())

    # Add date/time breakdowns (good for partitioning downstream)
    transformed_df = cleaned_df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
                               .withColumn("pickup_time", date_format(col("tpep_pickup_datetime"), "HH:mm:ss")) \
                               .withColumn("dropoff_date", to_date(col("tpep_dropoff_datetime"))) \
                               .withColumn("dropoff_time", date_format(col("tpep_dropoff_datetime"), "HH:mm:ss"))

    print("Debug: Transformed DataFrame schema")
    transformed_df.printSchema()

    output_path = f"yellow_tripdata_{year}_transformed.parquet"
    print(f" Writing transformed data to: {output_path}")

    # Partitioned Parquet (much better for querying big data)
    transformed_df.write.partitionBy("pickup_date").mode("overwrite").parquet(output_path)

    spark.stop()
    print(" Transformation complete, Parquet file written successfully")
    return output_path

# ------------------------------
# 4. Load Parquet -> Snowflake
# ------------------------------
def load_to_snowflake(parquet_path, snowflake_conn_id='snowflakes_con'):
    """Load transformed parquet data into Snowflake using SnowflakeHook."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    import logging
    import os
    from pyspark.sql import SparkSession

    logger = logging.getLogger(__name__)

    if not isinstance(snowflake_conn_id, str):
        raise ValueError(f"snowflake_conn_id must be a string, got {type(snowflake_conn_id)}: {snowflake_conn_id}")

    spark = SparkSession.builder \
        .appName("YellowTaxiToSnowflake") \
        .master("spark://23733ff5f9d2:7077") \
        .config("spark.sql.files.maxPartitionBytes", "256MB") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()

    print(f" Reading parquet: {parquet_path}")
    df = spark.read.parquet(parquet_path)

    # Initialize SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Get connection details for logging
    from airflow.sdk import Connection
    conn = Connection.get(snowflake_conn_id)
    conn_params = conn.extra_dejson
    logger.info(f"Snowflake connection parameters: {conn_params}")

    # Create a temporary stage in Snowflake
    stage_name = "TEMP_YELLOW_TAXI_STAGE"
    database = conn_params.get("database", conn.schema or "YELLOW_DATA")
    schema = conn_params.get("schema", "YELLO_ALL_DATA")
    table_name = f"{database}.{schema}.YELLOW_TRIP_2019_DATA"

    try:
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        logger.info(f"Ensuring table {table_name} exists")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            VendorID INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance DOUBLE,
            RatecodeID INT,
            store_and_fwd_flag STRING,
            PULocationID INT,
            DOLocationID INT,
            payment_type INT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            pickup_date DATE,
            pickup_time STRING,
            dropoff_date DATE,
            dropoff_time STRING
        )
        """
        cursor.execute(create_table_query)
        logger.info(f"Table {table_name} created or verified")

        # Create temporary stage
        logger.info(f"Creating temporary stage: {stage_name}")
        cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")

        # Collect all Parquet files
        parquet_files = []
        for root, _, files in os.walk(parquet_path):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, file))

        if not parquet_files:
            raise ValueError(f"No Parquet files found in {parquet_path}")

        # Upload each Parquet file to the stage
        for file_path in parquet_files:
            logger.info(f"Uploading {file_path} to Snowflake stage {stage_name}")
            cursor.execute(f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE")

        # Load data into Snowflake using COPY INTO
        logger.info(f"Loading data into {table_name}")
        copy_query = f"""
        COPY INTO {table_name}
        FROM @{stage_name}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
        cursor.execute(copy_query)

        # Verify the load by counting rows
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        logger.info(f"Loaded {row_count} rows into {table_name}")

        # Clean up
        cursor.execute(f"DROP STAGE {stage_name}")
        cursor.close()
        conn.close()
        print(" Data successfully written to Snowflake")

    except Exception as e:
        logger.error(f"Error loading to Snowflake: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()
# ------------------------------
# 5. Main flow
# ------------------------------
if __name__ == "__main__":
    year = 2019
    download_files(year)
    parquet_path = process_to_parquet(year)
    transformed_parquet_path = transform_taxi_data(parquet_path, year)
    load_to_snowflake(transformed_parquet_path, snowflake_conn_id='snowflakes_con')