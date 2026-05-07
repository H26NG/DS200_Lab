from pyspark import SparkContext, SparkConf
import os

# Khởi tạo SparkContext
conf = SparkConf().setAppName("DS200_Lab3_Bai2").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

print("SparkContext đã được khởi tạo thành công.")
print(f"Spark version: {sc.version}")

DATA_DIR = "."
OUTPUT_DIR = "."

movies_rdd = sc.textFile(os.path.join(DATA_DIR, "movies (1).txt"))
ratings_1_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_1 (1).txt"))
ratings_2_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_2 (1).txt"))

def parse_movie(line):
    """
    movies.txt schema:
    MovieID,Title,Genres

    Ví dụ:
    1,Toy Story (1995),Animation|Children's|Comedy

    Dùng rsplit để tránh lỗi nếu title có dấu phẩy.
    """
    try:
        line = line.strip()

        if "::" in line:
            parts = line.split("::")
            movie_id = int(parts[0])
            title = parts[1].strip()
            genres = parts[2].strip().split("|")
        else:
            movie_id, rest = line.split(",", 1)
            title, genres_raw = rest.rsplit(",", 1)

            movie_id = int(movie_id)
            title = title.strip()
            genres = genres_raw.strip().split("|")

        genres = [genre.strip() for genre in genres if genre.strip()]

        return movie_id, title, genres

    except:
        return None


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


# BƯỚC 1: Tạo map MovieID -> List of Genres

movie_genres_rdd = movies_rdd \
    .map(parse_movie) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], x[2]))

# movie_genres_rdd:
# MovieID -> [Genre1, Genre2, ...]

# BƯỚC 2: Map từ MovieID -> Rating

all_ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

movie_rating_rdd = all_ratings_rdd \
    .map(parse_rating) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[1], x[2]))

# movie_rating_rdd:
# MovieID -> Rating

# BƯỚC 3: Join MovieID -> Rating với MovieID -> List of Genres

movie_rating_genres_rdd = movie_rating_rdd.join(movie_genres_rdd)

genre_rating_rdd = movie_rating_genres_rdd.flatMap(
    lambda x: [
        (genre, (x[1][0], 1))
        for genre in x[1][1]
    ]
)

#Reduce để tính tổng điểm và số lượt đánh giá theo từng genre

genre_sum_count_rdd = genre_rating_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# BƯỚC 3: Tính điểm trung bình từng thể loại

genre_avg_count_rdd = genre_sum_count_rdd.mapValues(
    lambda v: (v[0] / v[1], v[1])
)

# KẾT XUẤT VÀ GHI FILE


# Sắp xếp:
# 1. AvgRating giảm dần
# 2. Count giảm dần
# 3. Tên genre tăng dần
output_log_path = os.path.join(OUTPUT_DIR, "Bai2.txt")
sorted_genre_rdd = genre_avg_count_rdd.sortBy(
    lambda x: (-x[1][0], -x[1][1], x[0])
)

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("BÀI 2: PHÂN TÍCH ĐÁNH GIÁ THEO THỂ LOẠI\n")
    f.write("=" * 70 + "\n\n")

    f.write("ĐIỂM TRUNG BÌNH THEO TỪNG THỂ LOẠI PHIM\n")
    f.write(f"{'Genre':<25} {'AvgRating':>12} {'Count':>10}\n")
    f.write("-" * 70 + "\n")

    for genre, (avg, cnt) in sorted_genre_rdd.toLocalIterator():
        line = f"{genre:<25} {avg:>12.2f} {cnt:>10}"
        print(line)
        f.write(line + "\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

sc.stop()