from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, LongType, TimestampType, DoubleType
import pyspark.sql.functions as F


# Kafka Configuration
KAFKA_SERVERS = 'ec2-44-192-38-38.compute-1.amazonaws.com:9092'
STREAM_TOPIC = 'reviews-stream'  #the source kafka topic from where it will read new reviews as they are generated
SUMMARY_TOPIC_TIME_WINDOWED = 'summary-results' #the destination kafka topic where this will update the summary results

#summary result configs
TIME_WINDOW = "1 day"  #time window for summary results


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SteamReviewsStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# Define Schema for incoming data
schema = StructType([
    StructField("index", LongType(), False),
    StructField("app_id", LongType(), False),
    StructField("app_name", StringType(), False),
    StructField("review_id", LongType(), False),
    StructField("language", StringType(), True),
    StructField("review", StringType(), True),
    StructField("timestamp_created", LongType(), True),
    StructField("timestamp_updated", LongType(), True),
    StructField("recommended", StringType(), True),
    StructField("votes_helpful", IntegerType(), True),
    StructField("votes_funny", IntegerType(), True),
    StructField("weighted_vote_score", FloatType(), True),
    StructField("comment_count", IntegerType(), True),
    StructField("steam_purchase", BooleanType(), True),
    StructField("received_for_free", BooleanType(), True),
    StructField("written_during_early_access", BooleanType(), True),
    StructField("author_steamid", StringType(), True),
    StructField("author_num_games_owned", IntegerType(), True),
    StructField("author_num_reviews", IntegerType(), True),
    StructField("author_playtime_forever", DoubleType(), True),
    StructField("author_playtime_last_two_weeks", DoubleType(), True),
    StructField("author_playtime_at_review", DoubleType(), True),
    StructField("author_last_played", FloatType(), True),
    StructField("sentiment", FloatType(), True)
])

# Read Stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", STREAM_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data")) \
    .select("data.*")

# Convert values to proper format <only for columns that will be outputted later for performance>
df = df.withColumn("timestamp_created", F.from_unixtime(F.col("timestamp_created")).cast(TimestampType())) \
       .withColumn("author_playtime_at_review", F.col("author_playtime_at_review") / 60)\
       .withColumn("recommended", F.when(F.col("recommended") == "true", True) \
                   .when(F.col("recommended") == "false", False) \
                   .otherwise(False)) \
       .withColumn("is_positive", F.col("sentiment") > 0.0)\
       .withColumn("is_negative", F.col("sentiment") < 0.0) 


#Calculate the required aggregate values for various attributes in the time window Window
windowed_df = df \
    .groupBy(
        F.window(F.col("timestamp_created"), TIME_WINDOW),
        F.col("app_id"),
        F.col("app_name")
    ) \
    .agg(
        F.avg("sentiment").alias("A_sentiment"),
        F.count("recommended").alias("T_reviews"),
        F.sum(F.col("recommended").cast("int")).alias("T_recommendations"),
        F.avg("author_playtime_at_review").alias("A_playtime"),
        F.sum(F.col("is_positive").cast("int")).alias("T_pos_reviews"),
        F.sum(F.col("is_negative").cast("int")).alias("T_neg_reviews"),
    ) \
    .select(
        F.col("app_id"),
        F.col("app_name"),
        F.col("window.end").alias("time"),
        F.col("A_playtime"),
        F.col("A_sentiment"),
        F.col("T_reviews"),
        F.col("T_recommendations"),
        F.col("T_pos_reviews"),
        F.col("T_neg_reviews"),
    )

#Write Sentiment Analysis Result to Kafka
windowed_df \
    .selectExpr("CAST(app_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .outputMode("update") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("topic", SUMMARY_TOPIC_TIME_WINDOWED) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_sentiment") \
    .start()


# Wait for the streaming queries to finish
spark.streams.awaitAnyTermination()