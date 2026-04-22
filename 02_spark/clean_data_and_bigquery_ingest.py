import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, to_date
import json
import os
from pathlib import Path
import time

start_time = time.perf_counter()

# Check are config files set.
config_directory = Path("/app/config")
required_files = ["config.json", "credentials.json"]

missing_files = []

for file_name in required_files:
    if not (config_directory / file_name).exists():
        missing_files.append(file_name)

if missing_files:
    raise FileNotFoundError(
        f"Missing required configuration files: {missing_files}"
    )

# Functions 

# Retry 5 times to insert data in BigQuery for case of errors
def retry(operation, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[SPARK] RETRY - Attempt {attempt}")
            return operation()

        except Exception as e:
            print(f"[SPARK] RETRY - Failed attempt {attempt}: {e}")

            # Fatal errors, stop retry
            if "403" in str(e) or "404" in str(e):
                raise

            if attempt == max_retries:
                raise

            time.sleep(5 * attempt)

def write_weather_data():
    df.write \
        .format("bigquery") \
        .option("table", f"{big_query_project}.{big_query_dataset}.weather_data") \
        .option("temporaryGcsBucket", bucket_name) \
        .mode("overwrite") \
        .save()

def write_stations_data():
    df_stations.write \
        .format("bigquery") \
        .option("table", f"{big_query_project}.{big_query_dataset}.stations") \
        .option("temporaryGcsBucket", bucket_name) \
        .mode("overwrite") \
        .save()

def write_capitals_data():
    df_capitals.write \
        .format("bigquery") \
        .option("table", f"{big_query_project}.{big_query_dataset}.capitals") \
        .option("temporaryGcsBucket", bucket_name) \
        .mode("overwrite") \
        .save()

# Start execution
spark = ( SparkSession.builder
    .master('local[*]')
    .appName('test')
    .config(
        "spark.jars",
        "/opt/spark/jars/gcs-connector-hadoop3-2.2.24.jar,"
        "/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar"
    )
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    .getOrCreate()
)

spark._jsc.hadoopConfiguration().set(
    "fs.gs.impl",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
)

spark._jsc.hadoopConfiguration().set(
    "fs.AbstractFileSystem.gs.impl",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
)

# Check are config files set.
config_directory = Path("/app/config")
required_files = ["config.json", "credentials.json"]

missing_files = []

for file_name in required_files:
    if not (config_directory / file_name).exists():
        missing_files.append(file_name)

if missing_files:
    raise FileNotFoundError(
        f"Missing required configuration files: {missing_files}"
    )

p = config_directory / 'config.json'

with open(p) as f:
    config = json.load(f)

bucket_name = str(config["gcs"]["bucket"])

big_query_project = str(config["gcs"]["big-query-project"])
big_query_dataset = str(config["gcs"]["big-query-dataset"])

# Prepare weather data
print("[SPARK] Started weather data conversion")

df = spark.read \
    .option("header", True) \
    .option("nullValue", "") \
    .csv(f"gs://{bucket_name}/weather_result/*/*")

df = df.withColumn("station", col("station").cast(StringType()) ) \
       .withColumn("time", to_date(col("time"), "yyyy-MM-dd").cast(DateType())) \
       .withColumn("temp", col("temp").cast(DecimalType(10, 2))) \
       .withColumn("tmax", col("tmax").cast(DecimalType(10, 2))) \
       .withColumn("tmin", col("tmin").cast(DecimalType(10, 2))) \
       .withColumn("rhum", col("rhum").cast(IntegerType())) \
       .withColumn("prcp", col("prcp").cast(DecimalType(10, 2))) \
       .withColumn("snwd", col("snwd").cast(DecimalType(10, 2))) \
       .withColumn("wspd", col("wspd").cast(DecimalType(10, 2))) \
       .withColumn("wpgt", col("wpgt").cast(DecimalType(10, 2))) \
       .withColumn("pres", col("pres").cast(DecimalType(10, 2))) \
       .withColumn("tsun", col("tsun").cast(DecimalType(10, 2))) \
       .withColumn("cldc", col("cldc").cast(DecimalType(10, 2)))

# Indexing
window = Window.partitionBy("station").orderBy("time")
df = df.withColumn("id", row_number().over(window).cast("long"))

df = df.select(
    "id",
    "station",
    "time",
    "temp",
    "tmax",
    "tmin",
    "rhum",
    "prcp",
    "snwd",
    "wspd",
    "wpgt",
    "pres",
    "tsun",
    "cldc"
)

# Split in 4 partition
df = df.repartition(4)
df.write.mode("overwrite").parquet(f"gs://{bucket_name}/result_parquet/weather_data/")

# Capitals
print("[SPARK] Started capitals data conversion")

schema_capitals = StructType([
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("capital", StringType(), True),
    StructField("lat", DecimalType(10, 4), True),
    StructField("lon", DecimalType(10, 4), True),
    StructField("iso2", StringType(), True),
])

file_path = f"gs://{bucket_name}/seeds/capitals.csv"

df_capitals = spark.read \
    .option("header", True) \
    .schema(schema_capitals) \
    .csv(file_path)

df_capitals.coalesce(1).write \
    .mode("overwrite") \
    .parquet(f"gs://{bucket_name}/result_parquet/capitals/")

# Stations
print("[SPARK] Started stations data conversion")


schema_stations = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("latitude", DecimalType(10, 4), True),
    StructField("longitude", DecimalType(10, 4), True),
    StructField("elevation", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("distance", DecimalType(10, 1), True),
    StructField("capital_id", IntegerType(), True),
])

file_path = f"gs://{bucket_name}/seeds/all_stations_data.csv"

df_stations = spark.read \
    .option("header", True) \
    .schema(schema_stations) \
    .csv(file_path)

df_stations.coalesce(1).write \
    .mode("overwrite") \
    .parquet(f"gs://{bucket_name}/result_parquet/stations/")

# Write weather data to BigQuery
print("[SPARK] Inserting weather data to BigQuery")
df = spark.read.parquet(f"gs://{bucket_name}/result_parquet/weather_data/")

retry(write_weather_data)

# Write stations to BigQuery
print("[SPARK] Inserting stations to BigQuery")
df_stations = spark.read.parquet(f"gs://{bucket_name}/result_parquet/stations/")
    
retry(write_stations_data)

# Write capitals to BigQuery
print("[SPARK] Inserting capitals to BigQuery")
df_capitals = spark.read.parquet(f"gs://{bucket_name}/result_parquet/capitals/")
    
retry(write_capitals_data)

spark.stop() # Clear resources

end_time = time.perf_counter()

print(f"[SPARK] Execution time: {end_time - start_time:.2f} seconds")





