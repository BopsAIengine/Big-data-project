import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, udf, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType, FloatType
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import VectorUDT # Needed for working with VectorType columns like 'probability'
import json
from datetime import datetime, timezone
import time

# Import Elasticsearch client libraries
from elasticsearch import Elasticsearch, helpers

# --- Configuration ---
KAFKA_BROKER = "10.128.0.2:9092" # Thay bằng internal IP của máy ảo Kafka/Processor
KAFKA_TOPIC = "tiki-comments"
ELASTICSEARCH_HOST = "10.128.0.2" # Thay bằng internal IP của máy ảo Elasticsearch
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "tiki_comments_streaming"
CHECKPOINT_LOCATION = "/home/minhtrang11122k4/tiki-crawler/checkpoint/tiki_comments_streaming"
# Corrected ML_MODEL_PATH based on your directory structure
ML_MODEL_PATH = "/home/minhtrang11122k4/tiki-crawler/ml_model/spark_full_ml_pipeline_model"
# ---------------------

print("Initializing Spark session...")
# Initialize Spark Session
# Ensure consistent indentation here, typically 4 spaces per level
spark = SparkSession.builder \
    .appName("TikiCommentsStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print("Spark session created.")

# Define the schema for the Kafka message value (JSON string)
schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("comment_id", IntegerType()),
    StructField("content", StringType()),
    StructField("rating", IntegerType()),
    StructField("original_timestamp", StringType()),
    StructField("crawl_timestamp", StringType())
])

print(f"Reading stream from Kafka topic: {KAFKA_TOPIC} on {KAFKA_BROKER}")
# Read data from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Select the value field and cast it to String, then parse the JSON
# Also select Kafka's own timestamp for the record
parsed_stream_df = kafka_stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_value", "timestamp as kafka_message_timestamp") \
    .select(col("key"), from_json(col("json_value"), schema).alias("data"), col("kafka_message_timestamp")) \
    .select("key", "data.*", "kafka_message_timestamp") # Flatten the struct

print("Schema of parsed_stream_df (from Kafka):")
parsed_stream_df.printSchema()

# Add Kafka timestamp column
df_with_ts = parsed_stream_df.withColumn("kafka_timestamp", col("kafka_message_timestamp").cast(TimestampType()))


# --- ML Model Loading and Application ---
print(f"Loading ML model from {ML_MODEL_PATH}...")
ml_model = None
predictions_df = None # Initialize predictions_df outside the try block

try:
    ml_model = PipelineModel.load(ML_MODEL_PATH)
    print("ML model loaded successfully.")

   
    df_for_ml_input = df_with_ts.withColumn("content_clean", col("content")) #Tạo cloum mới content_clean từ column content
    print("Applying ML model transformation...")
    predictions_df = ml_model.transform(df_for_ml_input) # Transform toàn bộ dataframe từ df_with_ts
    print("ML model transformation applied. Schema after transformation:")
    predictions_df.printSchema()

 
 #Example:prob_vector = [0.1, 0.85, 0.05]  # [p_negative, p_positive, p_neutral]  ---> Trả về     prediction_label = 1.0   # Dự đoán là tích cực
    def get_prob_of_predicted_class(prob_vector, prediction_label):  #Tính xác suất của lớp được dự đoán 
        if prob_vector is None or prediction_label is None:  
            return None
        try:
            if prediction_label is not None:
                label_index = int(prediction_label)
                if 0 <= label_index < len(prob_vector):
                    return float(prob_vector[label_index]) # Trả về xác suất của lớp được dự đoán
                else:
                    return None
            else:
                return None
        except (IndexError, TypeError, ValueError) as e:
            return None

    #udf : Hàm tự định để sử dụng, thực hiện những logic không có sẵn trong spark
    get_prob_udf = udf(get_prob_of_predicted_class, DoubleType())
    predictions_df = predictions_df.withColumn( # .withColumn : Thêm cột mới hoặc thay thế cột hiện có trong DataFrame
        "ml_prediction_probability", get_prob_udf(col("probability"), col("prediction"))
    )

    processed_stream_df = predictions_df \
        .withColumn("ml_prediction_label", col("prediction").cast(DoubleType())) \
        .withColumn("ml_predicted_rating",
                     when(col("prediction") == 0.0, lit(1))
                     .when(col("prediction") == 1.0, lit(5))
                     .otherwise(lit(3).cast(IntegerType()))
                    )

except Exception as e:
    print(f"Error loading or applying ML model: {e}. Proceeding without applying ML predictions.")
    processed_stream_df = df_with_ts \
        .withColumn("ml_prediction_label", lit(None).cast(DoubleType())) \
        .withColumn("ml_predicted_rating", lit(None).cast(IntegerType())) \
        .withColumn("ml_prediction_probability", lit(None).cast(DoubleType()))



final_stream_df = processed_stream_df.select(
    col("product_id"),
    col("comment_id"),
    col("content"), 
    col("original_timestamp"),
    col("crawl_timestamp"), 
    col("kafka_timestamp"),
    col("ml_prediction_label"),
    col("ml_predicted_rating"), 
    col("ml_prediction_probability") 

)

print("Final DataFrame schema before writing to Elasticsearch:")
final_stream_df.printSchema()

def process_batch_to_es(dataframe, batch_id):
    if dataframe.count() == 0:
        return
    try:
        comments_list = [row.asDict() for row in dataframe.collect()]
    except Exception as e:
        print(f"Error collecting DataFrame to list in batch {batch_id}: {e}")
        return


    actions = []
    for comment_dict in comments_list:
        if 'kafka_timestamp' in comment_dict and isinstance(comment_dict['kafka_timestamp'], datetime):
            comment_dict['kafka_timestamp'] = comment_dict['kafka_timestamp'].isoformat()
        doc_id = f"{comment_dict.get('product_id')}_{comment_dict.get('comment_id')}"
        action = {
            "_index": ELASTICSEARCH_INDEX,
            "_id": doc_id,
            "_source": comment_dict
        }
        actions.append(action)

    if not actions:
        return
    try:
        es_client = Elasticsearch(
            hosts=[{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}],
            request_timeout=60
        )
        if not es_client.indices.exists(index=ELASTICSEARCH_INDEX):
            print(f"Elasticsearch index '{ELASTICSEARCH_INDEX}' does not exist. Attempting to create.")
            try:
                # Define a simple mapping (adjust as needed)
                mapping = {
                    "properties": {
                        "product_id": {"type": "integer"},
                        "comment_id": {"type": "integer"},
                        "content": {"type": "text", "analyzer": "standard"},
                        "rating": {"type": "integer"},
                        "original_timestamp": {"type": "keyword"}, # Use keyword if not doing date math, or date
                        "crawl_timestamp": {"type": "keyword"},    # Use keyword if not doing date math, or date
                        "kafka_timestamp": {"type": "date"}, # ES can parse ISO strings
                        "ml_prediction_label": {"type": "double"}, # Mapping for ML prediction label (DoubleType)
                        "ml_predicted_rating": {"type": "integer"}, # Mapping for ML predicted rating (IntegerType)
                        "ml_prediction_probability": {"type": "double"} # Mapping for ML prediction probability (DoubleType)
                    }
                }
                # Use ignore=[400] instead of ignore=400 for list of status codes
                es_client.indices.create(index=ELASTICSEARCH_INDEX, mappings=mapping, ignore=[400]) # 'resource_already_exists_exception'
                print(f"Elasticsearch index '{ELASTICSEARCH_INDEX}' creation attempted.")
            except Exception as e_create:
                 print(f"Error creating Elasticsearch index '{ELASTICSEARCH_INDEX}': {e_create}")

        success_count, errors = helpers.bulk(es_client, actions, chunk_size=500, request_timeout=60, raise_on_error=False) # Increased timeout slightly
        print(f"Bulk indexing for batch {batch_id}: Successfully indexed {success_count} documents.")
        if errors:
            print(f"Encountered {len(errors)} errors during bulk indexing for batch {batch_id}. First 5 errors:")
            for i, error_info in enumerate(errors[:5]):
                print(f"Error {i+1}: {error_info}")

    except Exception as e:
        print(f"!!! UNHANDLED ERROR during Elasticsearch operation for batch {batch_id}: {e}")
    finally:
        pass # dataframe.unpersist() # Unpersist if persisted


print(f"Starting to write stream to Elasticsearch index: {ELASTICSEARCH_INDEX} using foreachBatch")
query = final_stream_df.writeStream \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .foreachBatch(process_batch_to_es) \
    .start()

print("writeStream.start() called. Streaming query initiated.")
print(f"Checkpoint location: {CHECKPOINT_LOCATION}")
print("Application will run until manually stopped or an unrecoverable error occurs.")
print("Calling query.awaitTermination()...")

query.awaitTermination()

print("Streaming query terminated.")
