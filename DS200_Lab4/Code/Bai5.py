"""
Lab 4 — Câu 5:
Thống kê điểm đánh giá trung bình, số lượng đánh giá theo từng mức (1-5).
Xử lý giá trị ngoại lệ và NULL trong cột Review_Score.
"""
import os
import sys
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, count, avg, round as spark_round, expr

RESULT_PATH = "/home/h26ng/Lab4/results/Bai5.txt"
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
    .appName("Lab4_Bai5")
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

reviews_df = read_csv(f"{DATA_DIR}/Order_Reviews.csv")

reviews_df = reviews_df.withColumn(
    "Review_Score", expr("try_cast(Review_Score as int)")
)


# Đếm tổng và NULL
total_reviews = reviews_df.count()
null_count = reviews_df.filter(col("Review_Score").isNull()).count()
outlier_count = reviews_df.filter(
    col("Review_Score").isNotNull() &
    ((col("Review_Score") < 1) | (col("Review_Score") > 5))
).count()
# Lọc bỏ NULL và giá trị ngoại lệ (chỉ giữ 1-5)
clean_reviews = reviews_df.filter(
    col("Review_Score").isNotNull() &
    (col("Review_Score") >= 1) &
    (col("Review_Score") <= 5)
)

# Điểm đánh giá trung bình chung
avg_score = clean_reviews.select(
    spark_round(avg("Review_Score"), 2).alias("Diem_Trung_Binh")
).collect()[0]["Diem_Trung_Binh"]

# Số lượng đánh giá theo từng mức
score_distribution = (
    clean_reviews
    .groupBy("Review_Score")
    .agg(count("*").alias("So_Luong"))
    .orderBy("Review_Score")
)

report = (
    f"Thống kê đánh giá \n"
    f"Tổng số đánh giá:              {total_reviews}\n"
    f"Số đánh giá NULL:               {null_count}\n"
    f"Số đánh giá ngoại lệ (<1 or >5): {outlier_count}\n"
    f"Số đánh giá hợp lệ (1-5):      {clean_reviews.count()}\n"
    f"Điểm đánh giá trung bình:      {avg_score}\n\n"
    f"Phân bố theo từng mức điểm \n"
    f"{show_as_string(score_distribution)}"
)

print(report, end="")

os.makedirs(os.path.dirname(RESULT_PATH), exist_ok=True)
with open(RESULT_PATH, "w", encoding="utf-8") as f:
    f.write(report)

print(f"\nĐã ghi kết quả: {RESULT_PATH}")
spark.stop()