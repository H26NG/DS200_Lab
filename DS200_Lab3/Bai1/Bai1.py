from pyspark import SparkContext, SparkConf
import os

# BÀI 1: Tính điểm trung bình và tổng số lượt đánh giá mỗi phim

# Khởi tạo SparkContext
conf = SparkConf().setAppName("DS200_Lab3_Bai1").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

print("SparkContext đã được khởi tạo thành công.")
print(f"Spark version: {sc.version}")

# ĐỌC DỮ LIỆU

DATA_DIR = "."

movies_rdd = sc.textFile(os.path.join(DATA_DIR, "movies (1).txt"))
ratings_1_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_1 (1).txt"))
ratings_2_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_2 (1).txt"))

# HÀM PARSE DỮ LIỆU

def parse_movie(line):
    """
    movies.txt schema:
    MovieID,Title,Genres

    Dùng rsplit để xử lý trường hợp tên phim có dấu phẩy.
    Ví dụ:
    11,American President, The (1995),Comedy|Drama|Romance
    """
    try:
        movie_id, rest = line.strip().split(",", 1)
        title, genres = rest.rsplit(",", 1)

        return int(movie_id), title.strip(), genres.strip()
    except:
        return None


def parse_rating(line):
    """
    ratings schema:
    UserID,MovieID,Rating,Timestamp
    """
    try:
        parts = line.strip().split(",")

        user_id = int(parts[0])
        movie_id = int(parts[1])
        rating = float(parts[2])
        timestamp = int(parts[3])

        return user_id, movie_id, rating, timestamp
    except:
        return None

# BƯỚC 1: TẠO MAP MovieID -> Title

movie_id_to_title = movies_rdd \
    .map(parse_movie) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], x[1])) \
    .collectAsMap()

print("\nMẫu movie_id_to_title:")
for movie_id, title in list(movie_id_to_title.items())[:5]:
    print(f"{movie_id}: {title}")

# BƯỚC 2: ĐỌC VÀ GỘP ratings_1 + ratings_2

all_ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

# MovieID -> (Rating, 1)
movie_rating_rdd = all_ratings_rdd \
    .map(parse_rating) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[1], (x[2], 1)))

# BƯỚC 3: REDUCE ĐỂ TÍNH TỔNG ĐIỂM VÀ SỐ LƯỢT ĐÁNH GIÁ

# MovieID -> (total_rating, count)
movie_rating_sum_rdd = movie_rating_rdd \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# BƯỚC 4: TÍNH ĐIỂM TRUNG BÌNH, LỌC PHIM CÓ ÍT NHẤT 5 LƯỢT ĐÁNH GIÁ

MIN_RATINGS = 5

# MovieID -> (avg_rating, count)
movie_avg_rdd = movie_rating_sum_rdd \
    .mapValues(lambda v: (round(v[0] / v[1], 2), v[1])) \
    .filter(lambda x: x[1][1] >= MIN_RATINGS)


# BƯỚC 5: THÊM TÊN PHIM VÀ HIỂN THỊ KẾT QUẢ

movie_title_bc = sc.broadcast(movie_id_to_title)

# (Title, AvgRating, Count)
movie_result_rdd = movie_avg_rdd \
    .map(lambda x: (
        movie_title_bc.value.get(x[0], f"Unknown({x[0]})"),
        x[1][0],
        x[1][1]
    ))

results = movie_result_rdd.collect()

# Sắp xếp:
# 1. Điểm trung bình giảm dần
# 2. Nếu bằng điểm, số lượt đánh giá giảm dần
# 3. Nếu vẫn bằng, sắp xếp theo tên phim
results_sorted = sorted(results, key=lambda x: (-x[1], -x[2], x[0]))

# IN TOÀN BỘ KẾT QUẢ

print("\n" + "=" * 85)
print(f"{'Tên phim':<65} {'Avg Rating':>10} {'Số lượt':>8}")
print("=" * 85)

for title, avg, count in results_sorted:
    print(f"{title:<65} {avg:>10.2f} {count:>8}")

# TÌM PHIM CÓ ĐIỂM TRUNG BÌNH CAO NHẤT TRONG NHÓM CÓ ÍT NHẤT 5 LƯỢT ĐÁNH GIÁ

best_movie = max(results_sorted, key=lambda x: (x[1], x[2]))

print("\n" + "=" * 85)
print(f"Phim có điểm trung bình cao nhất trong nhóm có ít nhất {MIN_RATINGS} lượt đánh giá:")
print(f"Tên phim: {best_movie[0]}")
print(f"Điểm trung bình: {best_movie[1]:.2f}")
print(f"Số lượt đánh giá: {best_movie[2]}")
print("=" * 85)

# XUẤT KẾT QUẢ RA FILE TXT
output_file = "Bai1.txt"

with open(output_file, "w", encoding="utf-8") as f:
    f.write("=" * 85 + "\n")
    f.write(f"{'Tên phim':<65} {'Avg Rating':>10} {'Số lượt':>8}\n")
    f.write("=" * 85 + "\n")

    for title, avg, count in results_sorted:
        f.write(f"{title:<65} {avg:>10.2f} {count:>8}\n")

    f.write("\n" + "=" * 85 + "\n")
    f.write(f"Phim có điểm trung bình cao nhất trong nhóm có ít nhất {MIN_RATINGS} lượt đánh giá:\n")
    f.write(f"Tên phim: {best_movie[0]}\n")
    f.write(f"Điểm trung bình: {best_movie[1]:.2f}\n")
    f.write(f"Số lượt đánh giá: {best_movie[2]}\n")
    f.write("=" * 85 + "\n")

print(f"\nĐã xuất kết quả ra file: {output_file}")

# Dừng SparkContext
sc.stop()