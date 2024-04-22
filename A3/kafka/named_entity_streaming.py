# RUN COMMAND:
# general: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_<scala-version>:<spark version> named_entity_streaming.py bootstrap-servers input-topic-name output-topic-name checkpoint-folder-path
# specific: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 named_entity_streaming.py localhost:9092 topic1 topic2 checkpoint_kafka

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count, concat_ws
import spacy

# Initialize spacy model
nlp = spacy.load('en_core_web_sm')

# Function to extract named entities from text
def extract_named_entities(text):
    doc = nlp(text)
    return ",".join([ent.text for ent in doc.ents])

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("""
        Usage: named_entity_streaming.py <bootstrap-servers> <input-kafka-topic> <output-kafka-topic> <checkpoint-folder-path>
        """, file=sys.stderr)
        sys.exit(-1)

    kafka_bootstrap_servers = sys.argv[1]
    input_kafka_topic = sys.argv[2]
    output_kafka_topic = sys.argv[3]
    checkpoint_path = sys.argv[4]

    # Initialize Spark session
    spark = SparkSession.builder.appName("NamedEntityCounter").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Read data from a Kafka topic
    input_stream = spark.readStream\
                    .format("kafka")\
                    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
                    .option("subscribe", input_kafka_topic)\
                    .load()\
                    .selectExpr("CAST(value AS STRING)")

    # Extract named entities from the "values" column
    # Kafka message content stored in column named "value"
    
    # spark.udf.register: register a user-defined function as a SQL function
    extract_named_entities_udf = spark.udf.register("extract_named_entities", extract_named_entities)
    
    stream_with_entities = input_stream.select(
                    explode(split(extract_named_entities_udf(input_stream.value), ",")
                    ).alias("named_entity")
    )

    # Perform a running count of named entities
    running_count = stream_with_entities.groupBy("named_entity").count()

    result_df = running_count.withColumn("named_entity_count", concat_ws(":", col("named_entity"), col("count")))\
                             .select("named_entity_count")

    # Write to results back to Kafka
    query = result_df\
                     .selectExpr("CAST(named_entity_count AS STRING) as value")\
                     .writeStream\
                     .format("kafka")\
                     .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
                     .option("topic", output_kafka_topic)\
                     .outputMode("complete")\
                     .option("checkpointLocation", checkpoint_path)\
                     .trigger(processingTime="15 minutes")\
                     .start()
    
    # Await termination of the streaming query
    query.awaitTermination()

