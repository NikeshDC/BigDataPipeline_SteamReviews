from kafka import KafkaProducer
import pandas as pd
import time, random, json


#configurations
KAFKA_SERVERS = ['localhost:9092']
DESTINATION_TOPIC = 'reviews-stream'

STREAM_BETWEEN_TIME = 5
STREAM_TIME_RANDOM = .1 * STREAM_BETWEEN_TIME

stream_datasource = 'sampled_steam_reviews_with_sentiment.csv'

headers = [
    "index",
    "app_id",
    "app_name",
    "review_id",
    "language",
    "review",
    "timestamp_created",
    "timestamp_updated",
    "recommended",
    "votes_helpful",
    "votes_funny",
    "weighted_vote_score",
    "comment_count",
    "steam_purchase",
    "received_for_free",
    "written_during_early_access",
    "author_steamid",
    "author_num_games_owned",
    "author_num_reviews",
    "author_playtime_forever",
    "author_playtime_last_two_weeks",
    "author_playtime_at_review",
    "author_last_played",
    "sentiment"
]


if __name__ == "__main__":
    # Kafka Producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS, 
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Load CSV
    data = pd.read_csv(stream_datasource, quotechar='"', sep=',', encoding='utf-8', escapechar='\\', header=None)
    data.columns = headers

    #write under kafka topic at random times
    for index, row in data.iterrows():
        producer.send(DESTINATION_TOPIC, value = row.to_dict())
        producer.flush()
        sleep_time = STREAM_BETWEEN_TIME + random.uniform(0, STREAM_TIME_RANDOM)
        print(index, " written")
        time.sleep(sleep_time)  # Add delay to simulate real-time stream generation
