from pyspark import SparkContext, SparkConf
from datetime import datetime, timezone
import os

conf = SparkConf().setAppName("DS200_Lab3_Bai6").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

print("SparkContext đã được khởi tạo thành công.")
print(f"Spark version: {sc.version}")

DATA_DIR = "."

ratings_1_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_1 (1).txt"))
ratings_2_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_2 (1).txt"))

def parse_rating(line):
    """
    ratings schema:
    UserID,MovieID,Rating,Timestamp
    """
    try:
        line = line.strip()

        if "::" in line:
            parts = line.split("::")
        else:
            parts = line.split(",")

        user_id = int(parts[0])
        movie_id = int(parts[1])
        rating = float(parts[2])
        timestamp = int(parts[3])

        return user_id, movie_id, rating, timestamp

    except:
        return None


def timestamp_to_year(timestamp):
    try:
        if timestamp > 10**12:
            timestamp = timestamp / 1000

        return datetime.fromtimestamp(timestamp, tz=timezone.utc).year

    except:
        return None

# Bước 1: Đọc dữ liệu ratings từ ratings_1.txt và ratings_2.txt

all_ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

parsed_ratings_rdd = all_ratings_rdd \
    .map(parse_rating) \
    .filter(lambda x: x is not None)

# parsed_ratings_rdd:
# UserID, MovieID, Rating, Timestamp

# Bước 2: Chuyển đổi Timestamp Unix thành Year
rating_with_year_rdd = parsed_ratings_rdd \
    .map(lambda x: (
        timestamp_to_year(x[3]),    # Year
        x[2]                        # Rating
    )) \
    .filter(lambda x: x[0] is not None)

# rating_with_year_rdd:
# Year -> Rating

# Bước 3: Với mỗi dòng rating, phát hành cặp key-value, key là năm, value là (rating, 1)

year_rating_rdd = rating_with_year_rdd.map(
    lambda x: (
        x[0],           # Year
        (x[1], 1)       # (Rating, 1)
    )
)

# year_rating_rdd:
# Year -> (Rating, 1)

# Bước 4: Reduce để tính tổng điểm và số lượt cho mỗi năm, sau đó tính trung bình rating

year_sum_count_rdd = year_rating_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

year_avg_count_rdd = year_sum_count_rdd.mapValues(
    lambda v: (v[0] / v[1], v[1])
)

# Sắp xếp theo năm tăng dần
sorted_result_rdd = year_avg_count_rdd.sortBy(
    lambda x: x[0]
)

output_log_path = "Bai6.txt"
with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("BÀI 6: PHÂN TÍCH ĐÁNH GIÁ THEO THỜI GIAN\n")
    f.write("=" * 70 + "\n\n")

    f.write("--- TỔNG SỐ LƯỢT ĐÁNH GIÁ VÀ ĐIỂM TRUNG BÌNH THEO NĂM ---\n")
    f.write(f"{'Year':<10} {'AvgRating':>12} {'Count':>10}\n")
    f.write("-" * 70 + "\n")

    for year, (avg, count) in sorted_result_rdd.toLocalIterator():
        line = f"{year:<10} {avg:>12.2f} {count:>10}"
        print(line)
        f.write(line + "\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

sc.stop()