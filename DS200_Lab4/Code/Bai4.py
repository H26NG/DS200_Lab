"""
Lab 4 — Câu 4:
Phân tích số lượng đơn hàng nhóm theo năm, tháng đặt hàng.
Hiển thị theo năm tăng dần, tháng giảm dần.
"""
import os
import sys
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count_distinct

RESULT_PATH = "/home/h26ng/Lab4/results/Bai4.txt"
DATA_DIR = "/home/h26ng/Lab4/Data"

def show_as_string(df, n=100):
    buf = StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        df.show(n, truncate=False)
    finally:
        sys.stdout = old
    return buf.getvalue()

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Lab4_Bai4")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def read_csv(path):
    return (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(path)
    )

orders_df = read_csv(f"{DATA_DIR}/Orders.csv")

orders_by_year_month = (
    orders_df
    .withColumn("Nam", year(col("Order_Purchase_Timestamp")))
    .withColumn("Thang", month(col("Order_Purchase_Timestamp")))
    .groupBy("Nam", "Thang")
    .agg(count_distinct("Order_ID").alias("So_Don_Hang"))
    .orderBy(col("Nam").asc(), col("Thang").asc())
)

report = (
    f"Số lượng đơn hàng theo năm, tháng\n"
    f"{show_as_string(orders_by_year_month)}"
)

print(report, end="")

os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
with open(RESULT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nĐã ghi kết quả: {RESULT_PATH}")
spark.stop()