import json
import os
from datetime import datetime

from pymongo import MongoClient, DESCENDING

from config import Config


def export_results(limit=500):
    client = MongoClient(Config.MONGO_URI)
    db = client[Config.MONGO_DB_NAME]
    collection = db[Config.MONGO_COLLECTION]

    docs = list(
        collection.find(
            {},
            {
                "_id": 0,
                "annotated_image": 0,  # bo qua anh base64 cho file gon
            }
        )
        .sort("stored_at", DESCENDING)
        .limit(limit)
    )

    if not docs:
        print("[Export] Khong co du lieu trong MongoDB.")
        return

    # Luu vao results/sample_output.json
    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "results",
        "sample_output.json"
    )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2, ensure_ascii=False)

    print(f"[Export] Da xuat {len(docs)} ban ghi ra {output_path}")
    print(f"[Export] Thoi gian export: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    client.close()


if __name__ == "__main__":
    export_results()