"""
config.py
---------
Cau hinh tap trung cho toan bo he thong People Counting.
Cac gia tri co the override bang environment variable, giup chay duoc
tren nhieu may (vd Windows chay camera, WSL chay cac node con lai)
ma khong can sua code.
"""

import os


class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    TOPIC_CAMERA_FRAMES = os.getenv("TOPIC_CAMERA_FRAMES", "camera_frames")
    TOPIC_DETECTION_RESULTS = os.getenv("TOPIC_DETECTION_RESULTS", "detection_results")

    TOPIC_NUM_PARTITIONS = int(os.getenv("TOPIC_NUM_PARTITIONS", "1"))
    TOPIC_REPLICATION_FACTOR = int(os.getenv("TOPIC_REPLICATION_FACTOR", "1"))

    DETECTOR_GROUP_ID = os.getenv("DETECTOR_GROUP_ID", "detector-group")
    STORAGE_GROUP_ID = os.getenv("STORAGE_GROUP_ID", "storage-group")

    # MongoDB
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "ds200_lab5")
    MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "results")

    # Camera / Producer-
    CAMERA_ID = os.getenv("CAMERA_ID", "H26NG")

    # Index webcam (DroidCam virtual cam thuong la 0 hoac 1)
    CAMERA_SOURCE = os.getenv("CAMERA_SOURCE", "0")

    # So frame gui di moi giay
    SEND_FPS = float(os.getenv("SEND_FPS", "2"))

    # Resize truoc khi encode de giam kich thuoc message Kafka
    FRAME_WIDTH = int(os.getenv("FRAME_WIDTH", "640"))
    FRAME_HEIGHT = int(os.getenv("FRAME_HEIGHT", "480"))

    JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "80"))

    # Detection (YOLOv8)
    YOLO_MODEL = os.getenv("YOLO_MODEL", "yolov8n.pt")

    # class_id = 0 la "person" trong COCO dataset
    PERSON_CLASS_ID = 0

    DETECTION_CONFIDENCE = float(os.getenv("DETECTION_CONFIDENCE", "0.4"))
    
    # API / Dashboard
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))

    DASHBOARD_HISTORY_SIZE = int(os.getenv("DASHBOARD_HISTORY_SIZE", "50"))

    # Spark analysis (doc tu MongoDB)
    SPARK_WINDOW_SECONDS = int(os.getenv("SPARK_WINDOW_SECONDS", "10"))

    @classmethod
    def camera_source_value(cls):
        """
        cv2.VideoCapture nhan int (webcam index) hoac str (duong dan file/RTSP).
        CAMERA_SOURCE luu duoi dang string, can convert sang int neu la so.
        """
        src = cls.CAMERA_SOURCE
        return int(src) if src.isdigit() else src