"""
Lab 4 — Câu 2:
Thống kê tổng số đơn hàng, số lượng khách hàng và người bán.
"""
import os
from pyspark.sql import SparkSession

RESULT_PATH = "/home/h26ng/Lab4/results/Bai2.txt"
DATA_DIR = "/home/h26ng/Lab4/Data"

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Lab4_Bai2")
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
order_items_df = read_csv(f"{DATA_DIR}/Order_Items.csv")

total_orders = orders_df.select("Order_ID").distinct().count()
total_customers = customers_df.select("Subscriber_ID").distinct().count()
total_sellers = order_items_df.select("Seller_ID").distinct().count()

report = (
    f"Thống kê tổng quan \n"
    f"Tổng số đơn hàng:      {total_orders}\n"
    f"Số lượng khách hàng:    {total_customers}\n"
    f"Số lượng người bán:     {total_sellers}\n"
)

print(report, end="")

os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
with open(RESULT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nĐã ghi kết quả: {RESULT_PATH}")
spark.stop()