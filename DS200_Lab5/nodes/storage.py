"""
nodes/storage.py
------------------
Storage Server: nhan ket qua detection tu Kafka topic detection_results
va luu vao MongoDB.

Chay (tu thu muc goc DS200_Lab5):
    python -m nodes.storage
"""

import time

from pymongo import MongoClient

from config import Config
from kafka_helper import JsonConsumer


class StorageNode:
    def __init__(self):
        self.consumer = JsonConsumer(
            topic=Config.TOPIC_DETECTION_RESULTS,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=Config.STORAGE_GROUP_ID,
        )

        self.client = MongoClient(Config.MONGO_URI)
        self.db = self.client[Config.MONGO_DB_NAME]
        self.collection = self.db[Config.MONGO_COLLECTION]

        print(f"[StorageNode] Ket noi MongoDB: {Config.MONGO_URI} "
              f"-> {Config.MONGO_DB_NAME}.{Config.MONGO_COLLECTION}")
        print("[StorageNode] San sang nhan ket qua...")

    def run(self):
        try:
            for message in self.consumer:
                data = message.value
                data["stored_at"] = time.time()

                self.collection.insert_one(data)

                print(f"[StorageNode] Da luu frame_id={data['frame_id']} "
                      f"camera_id={data['camera_id']} "
                      f"person_count={data['person_count']}")

        except KeyboardInterrupt:
            print("[StorageNode] Dung boi nguoi dung (Ctrl+C)")

        finally:
            self.consumer.close()
            self.client.close()


if __name__ == "__main__":
    node = StorageNode()
    node.run()