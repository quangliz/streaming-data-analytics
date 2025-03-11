import os
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, expr, 
    when, lit, to_timestamp, hour, minute, second
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, MapType, ArrayType, TimestampType
)
from dotenv import load_dotenv
from pymongo import MongoClient
import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'user_data')

# MongoDB configuration
MONGODB_HOST = os.getenv('MONGODB_HOST', 'mongodb')
MONGODB_PORT = int(os.getenv('MONGODB_PORT', '27017'))
MONGODB_USERNAME = os.getenv('MONGODB_USERNAME', 'admin')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD', 'admin')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'user_analytics')

# Define the schema for the user data from RandomUser.me API
user_schema = StructType([
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("location", StructType([
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("coordinates", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True)
    ]), True),
    StructField("nationality", StringType(), True),
    StructField("timestamp", StringType(), True)
])

def create_spark_session():
    # create spark session
    try:
        # create spark session
        spark = SparkSession.builder \
            .appName("UserDataProcessor") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        # set log level
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def get_mongodb_client():
    # create mongodb client
    try:
        # connect to mongodb
        client = MongoClient(
            host=MONGODB_HOST,
            port=MONGODB_PORT,
            username=MONGODB_USERNAME,
            password=MONGODB_PASSWORD,
            authSource='admin'
        )
        
        # get database
        db = client[MONGODB_DATABASE]
        
        # test connection
        db.command('ping')
        
        logger.info(f"Connected to MongoDB database: {MONGODB_DATABASE}")
        return client, db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def read_from_kafka(spark):
    #read streaming data from kafka
    try:
        # read from kafka
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        logger.info(f"Connected to Kafka topic: {KAFKA_TOPIC}")
        return kafka_stream
    except Exception as e:
        logger.error(f"Failed to read from Kafka: {e}")
        raise

def process_stream(kafka_stream):
    # process the stream
    try:
        # parse json data fromkafka
        parsed_stream = kafka_stream \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), user_schema).alias("user")) \
            .select("user.*") \
            .withColumn("processed_timestamp", to_timestamp(lit(expr("current_timestamp()"))))
        
        # extract location.country as a separate column for easier acces
        parsed_stream = parsed_stream.withColumn("country", col("location.country"))
        
        # create analytics
        
        # 1. Gender Distribution
        gender_distribution = parsed_stream \
            .groupBy("gender") \
            .count() \
            .select(
                col("gender"),
                col("count").alias("count"),
                to_timestamp(lit(expr("current_timestamp()"))).alias("timestamp")
            )
        
        # 2. Age Distribution (by age groups)
        age_distribution = parsed_stream \
            .select(
                when(col("age") < 18, "0-17")
                .when(col("age").between(18, 24), "18-24")
                .when(col("age").between(25, 34), "25-34")
                .when(col("age").between(35, 44), "35-44")
                .when(col("age").between(45, 54), "45-54")
                .when(col("age").between(55, 64), "55-64")
                .otherwise("65+").alias("age_group"),
                lit(1).alias("count"),
                to_timestamp(lit(expr("current_timestamp()"))).alias("timestamp")
            ) \
            .groupBy("age_group", "timestamp") \
            .sum("count") \
            .select(
                col("age_group"),
                col("sum(count)").alias("count"),
                col("timestamp")
            )
        
        # 3. Country Distribution
        country_distribution = parsed_stream \
            .groupBy("country") \
            .count() \
            .select(
                col("country"),
                col("count").alias("count"),
                to_timestamp(lit(expr("current_timestamp()"))).alias("timestamp")
            )
        
        # 4. User Inflow (count per minute)
        user_inflow = parsed_stream \
            .withWatermark("processed_timestamp", "1 minute") \
            .groupBy(window(col("processed_timestamp"), "1 minute")) \
            .count() \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("count").alias("count"),
                to_timestamp(lit(expr("current_timestamp()"))).alias("timestamp")
            )
        
        # 5. Average Age by Country
        avg_age_by_country = parsed_stream \
            .groupBy("country") \
            .agg(
                avg("age").alias("avg_age"),
                count("*").alias("count")
            ) \
            .select(
                col("country"),
                col("avg_age"),
                col("count"),
                to_timestamp(lit(expr("current_timestamp()"))).alias("timestamp")
            )
        
        return {
            "gender_distribution": gender_distribution,
            "age_distribution": age_distribution,
            "country_distribution": country_distribution,
            "user_inflow": user_inflow,
            "avg_age_by_country": avg_age_by_country
        }
    except Exception as e:
        logger.error(f"Error processing stream: {e}")
        raise

def write_to_mongodb(analytics_dfs, mongodb_client, mongodb_db):
    # write analytics to db
    query_list = []
    
    try:
        # convert df to mongodb documents
        def save_to_mongodb(batch_df, batch_id, collection_name):
            if batch_df.isEmpty():
                return
            
            # convert df to list of dictionaries
            rows = batch_df.collect()
            documents = []
            
            for row in rows:
                doc = row.asDict()
                # convert timestamp to datetime 
                if "timestamp" in doc and doc["timestamp"] is not None:
                    doc["timestamp"] = doc["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                if "window_start" in doc and doc["window_start"] is not None:
                    doc["window_start"] = doc["window_start"].strftime("%Y-%m-%d %H:%M:%S")
                if "window_end" in doc and doc["window_end"] is not None:
                    doc["window_end"] = doc["window_end"].strftime("%Y-%m-%d %H:%M:%S")
                
                # add batch_id and updated_at for tracking
                doc["batch_id"] = batch_id
                doc["updated_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                documents.append(doc)
            
            # insert documents into mongodb
            if documents:
                # clear previous data (for complete mode)
                mongodb_db[collection_name].delete_many({})
                # insert new data
                mongodb_db[collection_name].insert_many(documents)
                logger.info(f"Inserted {len(documents)} documents into {collection_name}")
        
        # Write gender distribution to MongoDB
        gender_query = analytics_dfs["gender_distribution"] \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda batch_df, batch_id: 
                save_to_mongodb(batch_df, batch_id, "gender_distribution")
            ) \
            .trigger(processingTime="10 seconds") \
            .start()
        query_list.append(gender_query)
        
        # Write age distribution to MongoDB
        age_query = analytics_dfs["age_distribution"] \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda batch_df, batch_id: 
                save_to_mongodb(batch_df, batch_id, "age_distribution")
            ) \
            .trigger(processingTime="10 seconds") \
            .start()
        query_list.append(age_query)
        
        # Write country distribution to MongoDB
        country_query = analytics_dfs["country_distribution"] \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda batch_df, batch_id: 
                save_to_mongodb(batch_df, batch_id, "country_distribution")
            ) \
            .trigger(processingTime="10 seconds") \
            .start()
        query_list.append(country_query)
        
        # Write user inflow to MongoDB
        inflow_query = analytics_dfs["user_inflow"] \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda batch_df, batch_id: 
                save_to_mongodb(batch_df, batch_id, "user_inflow")
            ) \
            .trigger(processingTime="10 seconds") \
            .start()
        query_list.append(inflow_query)
        
        # Write average age by country to MongoDB
        avg_age_query = analytics_dfs["avg_age_by_country"] \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda batch_df, batch_id: 
                save_to_mongodb(batch_df, batch_id, "avg_age_by_country")
            ) \
            .trigger(processingTime="10 seconds") \
            .start()
        query_list.append(avg_age_query)
        
        logger.info("Started writing analytics to MongoDB")
        return query_list
    except Exception as e:
        logger.error(f"Error writing to MongoDB: {e}")
        raise

def main():
    # main function
    logger.info("Starting Spark Processor...")
    
    # create spark session
    spark = create_spark_session()
    
    # create mongodb client
    mongodb_client, mongodb_db = get_mongodb_client()
    
    try:
        # read from kafka
        kafka_stream = read_from_kafka(spark)
        
        # process the stream
        analytics_dfs = process_stream(kafka_stream)
        
        # write to db
        query_list = write_to_mongodb(analytics_dfs, mongodb_client, mongodb_db)
        
        # wait for all queries to terminate
        for query in query_list:
            query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping the processor...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if mongodb_client:
            mongodb_client.close()
            logger.info("MongoDB connection closed")
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 