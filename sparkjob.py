from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *


import sys


# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("Vehicle Data Processing") \
    .getOrCreate()

# Σχήμα των εισερχομένων δεδομένων από Kafka
json_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("link", StringType(), True),
    StructField("name", StringType(), True),
    StructField("dn", IntegerType(), True),
    StructField("orig", StringType(), True),
    StructField("dest", StringType(), True),
    StructField("t", StringType(), True),
    StructField("x", DoubleType(), True),
    StructField("s", DoubleType(), True),
    StructField("v", StringType(), True),
])

# Ρυθμίσεις Kafka
bootstrapServers = "localhost:9092"
subscribeType = "subscribe"
topics = "test"

# Ανάγνωση δεδομένων από Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option(subscribeType, topics) \
    .load()

# Μετατροπή των δεδομένων JSON σε στήλες βάσει του json_schema
transformed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", json_schema).alias("data")) \
    .select("data.*")

# Επεξεργασία των δεδομένων για ομαδοποίηση
grouped_data = transformed_df \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(window("timestamp", "30 seconds", "30 seconds").alias("time_window"), col("link")) \
    .agg(avg("v").alias("vspeed"), count("name").alias("vcount"))

# Εγγραφή των αποτελεσμάτων στην κονσόλα
query = grouped_data.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

# Κλείσιμο του SparkSession
spark.stop()
