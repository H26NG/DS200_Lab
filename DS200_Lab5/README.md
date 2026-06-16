# Hệ thống đếm số lượng người qua camera (People Counting System)

## 1. Giới thiệu

Hệ thống đếm số lượng người hiện diện trong camera theo thời gian thực,
được xây dựng theo kiến trúc phân tán gồm 3 server độc lập, sử dụng
công nghệ dữ liệu lớn (Apache Kafka) để truyền dữ liệu giữa các server.

---

## 2. Kiến trúc hệ thống

```text
[Camera Server]         (Windows - nodes/camera.py)
  Đọc frame từ webcam (DroidCam)
  Encode JPEG/base64
  Gửi vào Kafka topic: camera_frames
          |
          v
  Kafka (Big Data Message Broker)
          |
          v
[Detection Server]      (WSL - nodes/detector.py)
  Nhận frame từ Kafka
  Chạy YOLOv8n → phát hiện người
  Vẽ bounding box
  Gửi kết quả vào Kafka topic: detection_results
          |
          v
[Storage Server]        (WSL - nodes/storage.py)
  Nhận kết quả từ Kafka
  Lưu vào MongoDB
          |
          v
[API Server]            (WSL - api/server.py)
  REST API + Live View trên browser
```

---

## 3. Công nghệ sử dụng

- **Python** - ngôn ngữ lập trình chính
- **Apache Kafka** — message broker, truyền dữ liệu streaming giữa các server
- **YOLOv8n** (Ultralytics) - mô hình phát hiện người (pretrained COCO)
- **OpenCV** - đọc webcam, encode/decode frame
- **MongoDB** - lưu trữ kết quả detection
- **Flask** - REST API và live view dashboard
- **PySpark** - phân tích batch data từ MongoDB (big data component)
- **Docker Compose** - chạy Kafka, Zookeeper, MongoDB

---

## 4. Ý nghĩa dữ liệu lớn

Dữ liệu từ camera là **streaming data** — dòng dữ liệu liên tục theo
thời gian thực. Apache Kafka được sử dụng làm **message broker** giúp:

- Tách rời hoàn toàn 3 server (camera, detector, storage)
- Xử lý dữ liệu dạng streaming liên tục
- Dễ mở rộng: thêm camera hoặc thêm detector chạy song song
- Đảm bảo dữ liệu không bị mất khi một server bị chậm (buffering)

PySpark (`scripts/spark_analysis.py`) được sử dụng để phân tích
batch data — tính thống kê số người theo từng phút, lưu kết quả
tổng hợp vào MongoDB.

---

## 5. Cấu trúc thư mục

```text
DS200_Lab5/
├── docker-compose.yml          # Kafka + Zookeeper + MongoDB
├── requirements.txt
├── config.py                   # Cấu hình tập trung
├── kafka_helper.py             # JsonProducer / JsonConsumer wrapper
├── nodes/
│   ├── camera.py               # Camera Server
│   ├── detector.py             # Detection Server (YOLOv8)
│   └── storage.py              # Storage Server (MongoDB)
├── api/
│   └── server.py               # Flask API + Live View
├── scripts/
│   ├── create_topics.py        # Tạo Kafka topics
│   ├── spark_analysis.py       # PySpark batch analysis
│   └── export_results.py       # Export MongoDB → JSON
└── results/
    └── sample_output.json      # Kết quả mẫu
```

---

## 6. Cài đặt môi trường

### Yêu cầu
- Windows + WSL2 (Ubuntu)
- Docker Desktop (WSL integration enabled)
- Python 3.10+
- DroidCam — do webcam laptop bị hỏng, sử dụng camera điện thoại
  kết nối qua DroidCam (WiFi) làm virtual webcam trên Windows

### Tạo môi trường Python (WSL)

```bash
cd ~/DS200_Lab/DS200_Lab5
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Khởi động Kafka + MongoDB

```bash
docker compose up -d
sleep 25 && docker ps
python -m scripts.create_topics
```

---

## 7. Cách chạy hệ thống

Mở **4 terminal** riêng biệt:

**Terminal 1 (WSL) — Storage Server:**
```bash
cd ~/DS200_Lab/DS200_Lab5 && source venv/bin/activate
python -m nodes.storage
```

**Terminal 2 (WSL) — Detection Server:**
```bash
cd ~/DS200_Lab/DS200_Lab5 && source venv/bin/activate
export DETECTION_CONFIDENCE=0.6
python -m nodes.detector
```

**Terminal 3 (WSL) — API Server:**
```bash
cd ~/DS200_Lab/DS200_Lab5 && source venv/bin/activate
python -m api.server
```

**Terminal 4 (Windows CMD) — Camera Server:**
```cmd
cd C:\camera-runner
venv\Scripts\activate
set KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
python \\wsl$\Ubuntu\home\h26ng\DS200_Lab\DS200_Lab5\nodes\camera.py
```

---

## 8. Các API endpoint

| Endpoint | Mô tả |
|---|---|
| `GET /` | Thông tin API |
| `GET /results` | Danh sách kết quả (mặc định 20 bản ghi) |
| `GET /results/<camera_id>` | Kết quả theo camera |
| `GET /latest` | Kết quả mới nhất |
| `GET /latest-frame` | Ảnh mới nhất đã vẽ bounding box |
| `GET /live` | Live view trên browser (tự refresh 1s) |
| `GET /stats/<camera_id>` | Thống kê theo camera |

---

## 9. Kết quả mẫu

Xem file `results/sample_output.json` - 500 bản ghi mới nhất được export
từ MongoDB, mỗi bản ghi có dạng:

```json
{
  "camera_id": "H26NG",
  "frame_id": 415,
  "source_timestamp": 1781577490.95,
  "processed_timestamp": 1781577490.85,
  "processing_time_ms": 93.38,
  "image_size": { "width": 640, "height": 480 },
  "person_count": 1,
  "boxes": [
    {
      "confidence": 0.9293,
      "bbox": { "x1": 142, "y1": 151, "x2": 529, "y2": 479 }
    }
  ],
  "stored_at": 1781577490.87
}
```

---

## 10. Phân tích Spark

Sau khi hệ thống đã chạy và có data trong MongoDB:

```bash
python -m scripts.spark_analysis
```

Kết quả phân tích (thống kê theo phút) được lưu vào MongoDB
collection `spark_analysis`.