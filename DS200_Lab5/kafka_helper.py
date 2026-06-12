"""
kafka_helper.py
Wrapper don gian quanh kafka-python, giup cac node (camera, detector,
storage) gui/nhan du lieu dang JSON ma khong can lap lai logic
serialize/deserialize va retry connection.
"""

import json
import time

from kafka import KafkaConsumer, KafkaProducer


class JsonProducer:
    """
    Producer gui du lieu dang dict Python, tu dong encode sang JSON bytes.
    """

    def __init__(self, bootstrap_servers, retries=5, retry_delay=3):
        self.bootstrap_servers = bootstrap_servers
        self.producer = self._connect(retries, retry_delay)

    def _connect(self, retries, retry_delay):
        """
        Kafka container co the chua san sang ngay khi script vua start,
        nen thu connect lai vai lan truoc khi bao loi.
        """
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                return KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
            except Exception as e:
                last_error = e
                print(f"[JsonProducer] Khong the ket noi Kafka "
                      f"(lan {attempt}/{retries}): {e}")
                time.sleep(retry_delay)

        raise ConnectionError(
            f"Không thể kết nối Kafka sau {retries} lần thử: {last_error}"
        )

    def send(self, topic, value, key=None):
        """
        Gui 1 message dang dict toi topic. Tra ve sau khi server xac nhan.
        """
        key_bytes = key.encode("utf-8") if key is not None else None
        future = self.producer.send(topic, value=value, key=key_bytes)
        return future.get(timeout=10)

    def close(self):
        self.producer.flush()
        self.producer.close()


class JsonConsumer:
    """
    Consumer doc message dang JSON bytes, tu dong decode thanh dict Python.
    """

    def __init__(self, topic, bootstrap_servers, group_id,
                 retries=5, retry_delay=3, auto_offset_reset="latest"):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = self._connect(retries, retry_delay, auto_offset_reset)

    def _connect(self, retries, retry_delay, auto_offset_reset):
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                return KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset=auto_offset_reset,
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                )
            except Exception as e:
                last_error = e
                print(f"[JsonConsumer] Khong the ket noi Kafka "
                      f"(lan {attempt}/{retries}): {e}")
                time.sleep(retry_delay)

        raise ConnectionError(
            f"Không thể kết nối Kafka sau {retries} lần thử: {last_error}"
        )

    def __iter__(self):
        """
        Cho phep dung truc tiep: for message in consumer_instance: ...
        message.value da duoc deserialize thanh dict.
        """
        for message in self.consumer:
            yield message

    def close(self):
        self.consumer.close()