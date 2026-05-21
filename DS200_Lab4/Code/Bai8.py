"""
Lab 4 — Câu 8:
Tính hiệu số giữa ngày giao hàng thực tế và ngày giao hàng dự kiến
để đánh giá hiệu suất giao hàng.
"""
import os
import sys
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, avg, count, sum as spark_sum, round as spark_round, when

RESULT_PATH = "/home/h26ng/Lab4/results/Bai8.txt"
DATA_DIR = "/home/h26ng/Lab4/Data"

def show_as_string(df, n=20):
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
    .appName("Lab4_Bai8")
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
order_items_df = read_csv(f"{DATA_DIR}/Order_Items.csv")

# Join Orders với Order_Items
delivery_df = (
    orders_df.join(order_items_df, on="Order_ID", how="inner")
    .filter(col("Order_Delivered_Carrier_Date").isNotNull())
    .withColumn(
        "Hieu_So_Ngay",
        datediff(
            col("Order_Delivered_Carrier_Date"),
            col("Shipping_Limit_Date")
        )
    )
)

# Thống kê tổng quan
total = delivery_df.count()
giao_som = delivery_df.filter(col("Hieu_So_Ngay") < 0).count()
giao_dung = delivery_df.filter(col("Hieu_So_Ngay") == 0).count()
giao_tre = delivery_df.filter(col("Hieu_So_Ngay") > 0).count()
avg_diff = delivery_df.select(spark_round(avg("Hieu_So_Ngay"), 2)).collect()[0][0]

# Phân bố hiệu số ngày
distribution = (
    delivery_df
    .withColumn("Trang_Thai", 
        when(col("Hieu_So_Ngay") < 0, "Giao som")
        .when(col("Hieu_So_Ngay") == 0, "Dung han")
        .otherwise("Giao tre")
    )
    .groupBy("Trang_Thai")
    .agg(
        count("*").alias("So_Luong"),
        spark_round(avg("Hieu_So_Ngay"), 2).alias("TB_Hieu_So_Ngay")
    )
    .orderBy("Trang_Thai")
)

report = (
    f"Đánh giá hiệu suất giao hàng \n"
    f"(Hiệu số = Ngày giao thực tế - Ngày giao dự kiến)\n"
    f"(Âm = giao sớm, 0 = đúng hạn, Dương = giao trễ)\n\n"
    f"Tổng đơn hàng đã giao:   {total}\n"
    f"Giao sớm:                {giao_som}\n"
    f"Đúng hạn:                {giao_dung}\n"
    f"Giao trễ:                {giao_tre}\n"
    f"Hiệu số trung bình:      {avg_diff} ngày\n\n"
    f"Phân bố theo trạng thái\n"
    f"{show_as_string(distribution)}"
)

print(report, end="")

os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
with open(RESULT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nĐã ghi kết quả: {RESULT_PATH}")
spark.stop()