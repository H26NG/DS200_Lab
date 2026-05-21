"""
Lab 4 — Câu 6:
Tính doanh thu (giá sản phẩm + phí vận chuyển) năm 2024, nhóm theo danh mục sản phẩm.
"""
import os
import sys
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum as spark_sum, round as spark_round

RESULT_PATH = "/home/h26ng/Lab4/results/Bai6.txt"
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
    .appName("Lab4_Bai6")
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
products_df = read_csv(f"{DATA_DIR}/Products.csv")

# Lọc đơn hàng năm 2024
orders_2024 = orders_df.filter(year(col("Order_Purchase_Timestamp")) == 2024)

# Join: Orders -> Order_Items -> Products
revenue_by_category = (
    orders_2024
    .join(order_items_df, on="Order_ID", how="inner")
    .join(products_df, on="Product_ID", how="inner")
    .withColumn("Doanh_Thu", col("Price") + col("Freight_Value"))
    .groupBy("Product_Category_Name")
    .agg(spark_round(spark_sum("Doanh_Thu"), 2).alias("Tong_Doanh_Thu"))
    .orderBy(col("Tong_Doanh_Thu").desc())
)

report = (
    f"Doanh thu năm 2024 theo danh mục sản phẩm \n"
    f"{show_as_string(revenue_by_category)}"
)

print(report, end="")

os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
with open(RESULT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nĐã ghi kết quả: {RESULT_PATH}")
spark.stop()