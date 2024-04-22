A Python application is created that reads from NewsAPI. The program uses a loop that gets real time data at periodic intervals of 5 minutes. This incoming data is continuously be written to a Kafka topic (e.g. topic1).

The following is required to run the code:
1. pip install newsapi-python
2. pip install kafka-python
3. Go to https://newsapi.org/ and click "Get API key" and follow the instructions to get your API key.

NOTE: The topic (e.g. topic1) MUST be created before the code is run. Refer to https://kafka.apache.org/quickstart and follow these directions to set up the Kafka topic.

RUN COMMAND: 
python news_to_kafka.py bootstrap-servers topic-name NewsAPI-key

Replace topic-name with the name of the Kafka topic you want to write News to. 

For me, the command is (replace MY-API-KEY with your key):
python news_to_kafka.py localhost:9092 topic1 MY-API-KEY

-------------------------------------------------------------------------------------------------

Next, a PySpark structured streaming application is created that continuously reads data from the Kafka topic above and keeps a running count of the named entities being mentioned. At the trigger time (for me, it is every 15 minutes), a message containing the named entities and their counts is sent to another Kafka topic (e.g. topic2).

The following is required to run the code:
1. Install Apache Spark.
2. pip install pyspark
3. pip install kafka-python
4. pip install spacy
5. python -m spacy download en_core_web_sm

NOTE: The topic (e.g. topic2) MUST be created before the code is run. Refer to https://kafka.apache.org/quickstart and follow these directions to set up the Kafka topic.

RUN COMMAND:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_<scala-version>:<spark version> named_entity_streaming.py bootstrap-servers input-topic-name output-topic-name checkpoint-folder-path

The --packages name MUST match the version of Spark and Scala on local machine. Run spark-shell to see their versions. The checkpoint-folder-path should be given as a relative path to the location of the .py file. NOTE: if you are using the same checkpoint folder name on subsequent runs of the code, the checkpoint folder may have to be deleted if the run command fails. 

For me, the command is:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 named_entity_streaming.py localhost:9092 topic1 topic2 checkpoint_kafka
