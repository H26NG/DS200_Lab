"""
scripts/create_topics.py
-------------------------
Tao 2 Kafka topic can thiet cho he thong: camera_frames va detection_results.

Chay (tu thu muc goc DS200_Lab5):
    python -m scripts.create_topics
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import Config


def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)

    topics = [
        NewTopic(
            name=Config.TOPIC_CAMERA_FRAMES,
            num_partitions=Config.TOPIC_NUM_PARTITIONS,
            replication_factor=Config.TOPIC_REPLICATION_FACTOR,
        ),
        NewTopic(
            name=Config.TOPIC_DETECTION_RESULTS,
            num_partitions=Config.TOPIC_NUM_PARTITIONS,
            replication_factor=Config.TOPIC_REPLICATION_FACTOR,
        ),
    ]

    for topic in topics:
        try:
            admin.create_topics([topic])
            print(f"Da tao topic: {topic.name}")
        except TopicAlreadyExistsError:
            print(f"Topic da ton tai, bo qua: {topic.name}")

    admin.close()


if __name__ == "__main__":
    create_topics()