# RUN COMMAND:
# general: python news_to_kafka.py bootstrap-servers topic-name NewsAPI-key
# specific: python news_to_kafka.py localhost:9092 topic1 MY-API-KEY

import sys
from kafka import KafkaProducer
from newsapi import NewsApiClient
import time

def fetch_and_send_news():
    try:
        # Fetch top headlines in the US from NewsAPI
        top_headlines = newsapi.get_top_headlines(country='us')

        # Send each article to Kafka
        for article in top_headlines.get("articles", []):
            headline = article.get("title", "")
            producer.send(kafka_topic, value=headline)
            print(f"Sent to Kafka: {headline}")
    
    except Exception as e:
        print(f"Error fetching/sending data: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: news_to_kafka.py <bootstrap-servers> <topic-name> <NewsAPI-key>
        """, file=sys.stderr)
        sys.exit(-1)

    kafka_bootstrap_servers = sys.argv[1]
    kafka_topic = sys.argv[2]
    newsapi = NewsApiClient(api_key=sys.argv[3])

    producer = KafkaProducer(bootstrap_servers = kafka_bootstrap_servers,
                             value_serializer = lambda x: str(x).encode("utf-8"))

    try:
        # Continuously fetch news data every 5 minutes until you stop the application manually (using Ctrl+C)
        # sleep is in seconds so 60 seconds/minute x 5 minutes = 300 seconds
        while True:
            fetch_and_send_news()
            time.sleep(300)

    except KeyboardInterrupt:
        print("Application stopped.")



