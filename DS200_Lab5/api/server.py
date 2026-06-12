"""
api/server.py
---------------
API Server (Flask): cung cap REST API va trang web live view de xem
ket qua tu MongoDB.

Chay (tu thu muc goc DS200_Lab5):
    python -m api.server
"""

import base64
import time

from flask import Flask, jsonify, request, Response, render_template_string
from pymongo import MongoClient, DESCENDING

from config import Config


app = Flask(__name__)

client = MongoClient(Config.MONGO_URI)
db = client[Config.MONGO_DB_NAME]
collection = db[Config.MONGO_COLLECTION]


def serialize_doc(doc, include_image=False):
    """
    MongoDB tra ve _id dang ObjectId, khong serialize JSON duoc truc tiep.
    Mac dinh bo qua field annotated_image vi base64 rat dai, lam JSON
    qua nang khi list nhieu ban ghi.
    """
    doc = dict(doc)
    doc["_id"] = str(doc["_id"])
    if not include_image:
        doc.pop("annotated_image", None)
    return doc


@app.route("/")
def index():
    return jsonify({
        "message": "People Counting System API",
        "endpoints": {
            "results": "/results?limit=20",
            "results_by_camera": "/results/<camera_id>",
            "latest": "/latest",
            "latest_frame": "/latest-frame",
            "live": "/live",
            "stats": "/stats/<camera_id>",
        }
    })


@app.route("/results")
def get_results():
    limit = int(request.args.get("limit", 20))
    docs = collection.find().sort("stored_at", DESCENDING).limit(limit)
    return jsonify([serialize_doc(d) for d in docs])


@app.route("/results/<camera_id>")
def get_results_by_camera(camera_id):
    limit = int(request.args.get("limit", 20))
    docs = (collection.find({"camera_id": camera_id})
            .sort("stored_at", DESCENDING).limit(limit))
    return jsonify([serialize_doc(d) for d in docs])


@app.route("/latest")
def get_latest():
    doc = collection.find_one(sort=[("stored_at", DESCENDING)])
    if doc is None:
        return jsonify({"message": "Chua co du lieu"}), 404
    return jsonify(serialize_doc(doc))


@app.route("/latest-frame")
def get_latest_frame():
    doc = collection.find_one(sort=[("stored_at", DESCENDING)])
    if doc is None or "annotated_image" not in doc:
        return jsonify({"message": "Chua co du lieu"}), 404

    image_bytes = base64.b64decode(doc["annotated_image"])
    return Response(image_bytes, mimetype="image/jpeg")


LIVE_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>People Counting - Live View</title>
    <meta http-equiv="refresh" content="1">
    <style>
        body { font-family: sans-serif; text-align: center;
               background: #111; color: #eee; }
        img { max-width: 90%; border: 2px solid #444; margin-top: 20px; }
    </style>
</head>
<body>
    <h2>People Counting - Live View ({{ camera_id }})</h2>
    <img src="/latest-frame?_={{ ts }}" />
</body>
</html>
"""


@app.route("/live")
def live_view():
    return render_template_string(LIVE_PAGE, ts=time.time(),
                                    camera_id=Config.CAMERA_ID)


@app.route("/stats/<camera_id>")
def get_stats(camera_id):
    pipeline = [
        {"$match": {"camera_id": camera_id}},
        {"$group": {
            "_id": "$camera_id",
            "total_frames": {"$sum": 1},
            "avg_person_count": {"$avg": "$person_count"},
            "max_person_count": {"$max": "$person_count"},
            "avg_processing_time_ms": {"$avg": "$processing_time_ms"},
        }}
    ]
    result = list(collection.aggregate(pipeline))
    if not result:
        return jsonify({"message": "Khong co du lieu cho camera nay"}), 404

    stats = result[0]
    stats.pop("_id")
    stats["camera_id"] = camera_id
    return jsonify(stats)


if __name__ == "__main__":
    app.run(host=Config.API_HOST, port=Config.API_PORT, debug=True)