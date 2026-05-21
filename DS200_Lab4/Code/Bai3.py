"""
Lab 4 — Câu 3:
Phân tích số lượng đơn hàng theo quốc gia, sắp xếp giảm dần.
"""
import os
import sys
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count_distinct

RESULT_PATH = "/home/h26ng/Lab4/results/Bai3.txt"
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
    .appName("Lab4_Bai3")
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
customers_df = read_csv(f"{DATA_DIR}/Customer_List.csv")

# Join orders với customers qua Customer_Trx_ID để lấy quốc gia
orders_by_country = (
    orders_df.join(customers_df, on="Customer_Trx_ID", how="inner")
    .groupBy("Customer_Country")
    .agg(count_distinct("Order_ID").alias("So_Don_Hang"))
    .orderBy(col("So_Don_Hang").desc())
)

report = (
    f"Số lượng đơn hàng theo quốc gia (giảm dần) \n"
    f"{show_as_string(orders_by_country)}"
)

print(report, end="")

os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
with open(RESULT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nĐã ghi kết quả: {RESULT_PATH}")
spark.stop()