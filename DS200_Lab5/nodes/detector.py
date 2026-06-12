"""
nodes/detector.py
-------------------
Detection Server: nhan frame tu Kafka topic camera_frames, chay YOLOv8
de phat hien nguoi (class "person"), ve bounding box va gui ket qua
sang Kafka topic detection_results.

Chay (tu thu muc goc DS200_Lab5):
    python -m nodes.detector
"""

import base64
import time

import cv2
import numpy as np
from ultralytics import YOLO

from config import Config
from kafka_helper import JsonConsumer, JsonProducer


class DetectorNode:
    def __init__(self):
        print(f"[DetectorNode] Dang load model {Config.YOLO_MODEL} ...")
        self.model = YOLO(Config.YOLO_MODEL)

        self.consumer = JsonConsumer(
            topic=Config.TOPIC_CAMERA_FRAMES,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=Config.DETECTOR_GROUP_ID,
        )
        self.producer = JsonProducer(Config.KAFKA_BOOTSTRAP_SERVERS)

        print("[DetectorNode] San sang nhan frame...")

    def _decode_frame(self, image_b64):
        image_bytes = base64.b64decode(image_b64)
        np_arr = np.frombuffer(image_bytes, dtype=np.uint8)
        return cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    def _encode_frame(self, frame):
        encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), Config.JPEG_QUALITY]
        ok, buffer = cv2.imencode(".jpg", frame, encode_params)
        if not ok:
            raise RuntimeError("Khong the encode frame thanh JPEG")
        return base64.b64encode(buffer).decode("utf-8")

    def _detect_persons(self, frame):
        """
        Chay YOLOv8 tren frame, chi giu lai detection co class = person
        va confidence >= threshold (Config.DETECTION_CONFIDENCE).
        """
        results = self.model(frame, verbose=False)[0]

        boxes = []
        for box in results.boxes:
            class_id = int(box.cls[0])
            confidence = float(box.conf[0])

            if class_id != Config.PERSON_CLASS_ID:
                continue
            if confidence < Config.DETECTION_CONFIDENCE:
                continue

            x1, y1, x2, y2 = box.xyxy[0].tolist()
            boxes.append({
                "confidence": round(confidence, 4),
                "bbox": {
                    "x1": int(x1), "y1": int(y1),
                    "x2": int(x2), "y2": int(y2),
                },
            })

        return boxes

    def _draw_boxes(self, frame, boxes):
        annotated = frame.copy()
        for item in boxes:
            b = item["bbox"]
            cv2.rectangle(annotated, (b["x1"], b["y1"]), (b["x2"], b["y2"]),
                          (0, 255, 0), 2)
            label = f"person {item['confidence']:.2f}"
            cv2.putText(annotated, label, (b["x1"], max(b["y1"] - 8, 0)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)

        count_label = f"Person count: {len(boxes)}"
        cv2.putText(annotated, count_label, (10, 25),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)
        return annotated

    def run(self):
        try:
            for message in self.consumer:
                data = message.value
                start = time.time()

                frame = self._decode_frame(data["image_data"])
                boxes = self._detect_persons(frame)
                annotated = self._draw_boxes(frame, boxes)
                annotated_b64 = self._encode_frame(annotated)

                processed_timestamp = time.time()
                processing_time_ms = round(
                    (processed_timestamp - start) * 1000, 2
                )

                result = {
                    "camera_id": data["camera_id"],
                    "frame_id": data["frame_id"],
                    "source_timestamp": data["timestamp"],
                    "processed_timestamp": processed_timestamp,
                    "processing_time_ms": processing_time_ms,
                    "image_size": {
                        "width": data["width"],
                        "height": data["height"],
                    },
                    "person_count": len(boxes),
                    "boxes": boxes,
                    "annotated_image": annotated_b64,
                }

                self.producer.send(Config.TOPIC_DETECTION_RESULTS, result,
                                    key=data["camera_id"])

                print(f"[DetectorNode] frame_id={data['frame_id']} "
                      f"person_count={len(boxes)} "
                      f"({processing_time_ms} ms)")

        except KeyboardInterrupt:
            print("[DetectorNode] Dung boi nguoi dung (Ctrl+C)")

        finally:
            self.consumer.close()
            self.producer.close()


if __name__ == "__main__":
    node = DetectorNode()
    node.run()