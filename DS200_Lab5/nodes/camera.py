"""
nodes/camera.py
----------------
Camera Server: doc frame tu webcam (hoac video file), encode JPEG/base64,
va gui vao Kafka topic camera_frames.

Chay (tu thu muc goc DS200_Lab5):
    python -m nodes.camera
    python -m nodes.camera --source 1          # doi camera index
    python -m nodes.camera --source video.mp4  # dung video file
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import base64
import time

import cv2

from config import Config
from kafka_helper import JsonProducer


class CameraNode:
    def __init__(self, source=None, fps=None, camera_id=None):
        self.source = source if source is not None else Config.camera_source_value()
        self.fps = fps if fps is not None else Config.SEND_FPS
        self.camera_id = camera_id if camera_id is not None else Config.CAMERA_ID

        self.cap = cv2.VideoCapture(self.source)
        if not self.cap.isOpened():
            raise RuntimeError(f"Khong the mo camera/video: {self.source}")

        self.producer = JsonProducer(Config.KAFKA_BOOTSTRAP_SERVERS)
        self.frame_id = 0

        print(f"[CameraNode] camera_id={self.camera_id}, source={self.source}, "
              f"send_fps={self.fps}")

    def _encode_frame(self, frame):
        # Resize de giam kich thuoc message gui qua Kafka
        frame = cv2.resize(frame, (Config.FRAME_WIDTH, Config.FRAME_HEIGHT))

        encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), Config.JPEG_QUALITY]
        ok, buffer = cv2.imencode(".jpg", frame, encode_params)
        if not ok:
            raise RuntimeError("Khong the encode frame thanh JPEG")

        image_b64 = base64.b64encode(buffer).decode("utf-8")
        height, width = frame.shape[:2]
        return image_b64, width, height

    def run(self):
        interval = 1.0 / self.fps

        try:
            while True:
                start = time.time()

                ok, frame = self.cap.read()
                if not ok:
                    print("[CameraNode] Khong doc duoc frame, dung lai.")
                    break

                image_b64, width, height = self._encode_frame(frame)

                message = {
                    "camera_id": self.camera_id,
                    "frame_id": self.frame_id,
                    "timestamp": time.time(),
                    "width": width,
                    "height": height,
                    "image_data": image_b64,
                }

                self.producer.send(Config.TOPIC_CAMERA_FRAMES, message,
                                    key=self.camera_id)
                print(f"[CameraNode] Da gui frame_id={self.frame_id}")

                self.frame_id += 1

                # Giu toc do gui ~ self.fps frame/giay
                elapsed = time.time() - start
                sleep_time = interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("[CameraNode] Dung boi nguoi dung (Ctrl+C)")

        finally:
            self.cap.release()
            self.producer.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Camera Server")
    parser.add_argument("--source", default=None,
                         help="Webcam index (so) hoac duong dan video file")
    parser.add_argument("--fps", type=float, default=None,
                         help="So frame gui di moi giay")
    args = parser.parse_args()

    source = args.source
    if source is not None and source.isdigit():
        source = int(source)

    return source, args.fps


if __name__ == "__main__":
    source, fps = parse_args()
    node = CameraNode(source=source, fps=fps)
    node.run()