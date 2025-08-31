from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("MusicStreamingPipeline") \
    .getOrCreate()

# Define the schema of incoming Kafka JSON messages
schema = StructType() \
    .add("user_id", StringType()) \
    .add("song", StringType()) \
    .add("timestamp", StringType()) \
    .add("listen_duration", IntegerType())

# Read from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music-events") \
    .load()

# Convert the value from Kafka (binary) to string, then parse json
value_df = df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON to columns
json_df = value_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Convert timestamp string to timestamp type
streaming_df = json_df.withColumn("event_time", col("timestamp").cast(TimestampType()))

# Perform simple aggregation (e.g. count listens per song in 1 minute windows)
agg_df = streaming_df.groupBy("song").count()

# Write aggregated output to console (for demo) or sink (to GCS, Pub/Sub, Snowflake)
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
