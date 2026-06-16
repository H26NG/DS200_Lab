from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, from_unixtime, \
    date_format
from pymongo import MongoClient

from config import Config


def get_spark_session():
    return (SparkSession.builder
            .appName("PeopleCountingAnalysis")
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            .getOrCreate())


def load_data_from_mongo(client):
    """
    Doc du lieu tu MongoDB bang pymongo, chuyen sang list of dict
    de tao Spark DataFrame.
    """
    db = client[Config.MONGO_DB_NAME]
    collection = db[Config.MONGO_COLLECTION]

    docs = list(collection.find(
        {},
        {
            "_id": 0,
            "camera_id": 1,
            "frame_id": 1,
            "person_count": 1,
            "processing_time_ms": 1,
            "processed_timestamp": 1,
        }
    ))

    if not docs:
        print("[SparkAnalysis] Khong co du lieu trong MongoDB.")
        return None

    print(f"[SparkAnalysis] Doc duoc {len(docs)} ban ghi tu MongoDB.")
    return docs


def run_analysis(spark, docs):
    df = spark.createDataFrame(docs)

    # Them cot thoi gian dang doc duoc
    df = df.withColumn(
        "datetime",
        date_format(from_unixtime("processed_timestamp"), "yyyy-MM-dd HH:mm")
    )

    print("\n=== THONG KE TONG QUAT ===")
    df.select(
        count("frame_id").alias("tong_so_frame"),
        avg("person_count").alias("so_nguoi_trung_binh"),
        max("person_count").alias("so_nguoi_cao_nhat"),
        min("person_count").alias("so_nguoi_thap_nhat"),
        avg("processing_time_ms").alias("thoi_gian_xu_ly_trung_binh_ms"),
    ).show(truncate=False)

    print("=== THONG KE THEO PHUT ===")
    df.groupBy("camera_id", "datetime") \
        .agg(
            count("frame_id").alias("so_frame"),
            avg("person_count").alias("so_nguoi_trung_binh"),
            max("person_count").alias("so_nguoi_cao_nhat"),
        ) \
        .orderBy("datetime") \
        .show(truncate=False)

    return df


def save_analysis_to_mongo(client, df):
    """
    Luu ket qua phan tich tong hop theo phut vao collection spark_analysis.
    """
    db = client[Config.MONGO_DB_NAME]
    out_collection = db["spark_analysis"]
    out_collection.drop()

    rows = df.groupBy("camera_id", "datetime") \
        .agg(
            count("frame_id").alias("so_frame"),
            avg("person_count").alias("so_nguoi_trung_binh"),
            max("person_count").alias("so_nguoi_cao_nhat"),
        ) \
        .orderBy("datetime") \
        .collect()

    records = [row.asDict() for row in rows]
    if records:
        out_collection.insert_many(records)
        print(f"\n[SparkAnalysis] Da luu {len(records)} ban ghi "
              f"vao collection 'spark_analysis'.")


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    client = MongoClient(Config.MONGO_URI)

    docs = load_data_from_mongo(client)
    if docs is None:
        return

    df = run_analysis(spark, docs)
    save_analysis_to_mongo(client, df)

    spark.stop()
    client.close()
    print("[SparkAnalysis] Hoan tat phan tich.")


if __name__ == "__main__":
    main()