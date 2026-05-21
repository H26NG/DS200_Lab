"""
Lab 4 — Câu 1:
Đọc dữ liệu từ các file CSV, tự suy kiểu (inferSchema) cho mỗi cột.
"""
import os
import sys
from io import StringIO
from pyspark.sql import SparkSession

RESULT_PATH = "/home/h26ng/Lab4/results/Bai1/Bai1.txt"
DATA_DIR = "/home/h26ng/Lab4/Data"

def schema_as_string(df):
    buf = StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        df.printSchema()
    finally:
        sys.stdout = old
    return buf.getvalue()

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Lab4_Bai1")
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
order_reviews_df = read_csv(f"{DATA_DIR}/Order_Reviews.csv")
products_df = read_csv(f"{DATA_DIR}/Products.csv")

tables = [
    ("Orders", orders_df),
    ("Customer_List", customers_df),
    ("Order_Items", order_items_df),
    ("Order_Reviews", order_reviews_df),
    ("Products", products_df),
]

parts = []
for title, df in tables:
    parts.append(f"{title}\n{schema_as_string(df).rstrip()}")

report = "\n\n".join(parts) + "\n"
print(report, end="")

os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
with open(RESULT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nĐã ghi kết quả: {RESULT_PATH}")
spark.stop()