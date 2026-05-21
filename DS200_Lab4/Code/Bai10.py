"""
Lab 4 — Câu 10:
Xếp hạng các seller dựa trên tổng doanh thu và số lượng đơn hàng bán được.
"""
import os
import sys
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count_distinct, sum as spark_sum, round as spark_round, dense_rank
from pyspark.sql.window import Window

RESULT_PATH = "/home/h26ng/Lab4/results/Bai10.txt"
DATA_DIR = "/home/h26ng/Lab4/Data"

def show_as_string(df, n=50):
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
    .appName("Lab4_Bai10")
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

order_items_df = read_csv(f"{DATA_DIR}/Order_Items.csv")

# Tính tổng doanh thu và số đơn hàng cho mỗi seller
seller_stats = (
    order_items_df
    .withColumn("Doanh_Thu", col("Price") + col("Freight_Value"))
    .groupBy("Seller_ID")
    .agg(
        spark_round(spark_sum("Doanh_Thu"), 2).alias("Tong_Doanh_Thu"),
        count_distinct("Order_ID").alias("So_Don_Hang")
    )
)

# Xếp hạng theo tổng doanh thu
window_revenue = Window.orderBy(col("Tong_Doanh_Thu").desc())
window_orders = Window.orderBy(col("So_Don_Hang").desc())

seller_ranked = (
    seller_stats
    .withColumn("Xep_Hang_Doanh_Thu", dense_rank().over(window_revenue))
    .withColumn("Xep_Hang_Don_Hang", dense_rank().over(window_orders))
    .orderBy(col("Tong_Doanh_Thu").desc())
)

report = (
    f"Xếp hạng Seller theo doanh thu và số đơn hàng \n"
    f"Tổng số seller: {seller_stats.count()}\n\n"
    f"Top 50 seller:\n"
    f"{show_as_string(seller_ranked, 50)}"
)

print(report, end="")

os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
with open(RESULT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nĐã ghi kết quả: {RESULT_PATH}")
spark.stop()