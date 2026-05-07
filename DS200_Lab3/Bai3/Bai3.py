from pyspark import SparkContext, SparkConf
import os

conf = SparkConf().setAppName("DS200_Lab3_Bai3").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

print("SparkContext đã được khởi tạo thành công.")
print(f"Spark version: {sc.version}")

DATA_DIR = "."

movies_rdd = sc.textFile(os.path.join(DATA_DIR, "movies (1).txt"))
ratings_1_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_1 (1).txt"))
ratings_2_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_2 (1).txt"))
users_rdd = sc.textFile(os.path.join(DATA_DIR, "users (1).txt"))


def parse_movie(line):
    """
    movies.txt schema:
    MovieID,Title,Genres
    """
    try:
        line = line.strip()

        if "::" in line:
            parts = line.split("::")
            movie_id = int(parts[0])
            title = parts[1].strip()
        else:
            movie_id, rest = line.split(",", 1)
            title, genres = rest.rsplit(",", 1)
            movie_id = int(movie_id)
            title = title.strip()

        return movie_id, title

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


def parse_user(line):
    """
    users.txt schema:
    UserID,Gender,Age,Occupation,Zip-code
    """
    try:
        line = line.strip()

        if "::" in line:
            parts = line.split("::")
        else:
            parts = line.split(",")

        user_id = int(parts[0])
        gender = parts[1].strip()
        age = int(parts[2])
        occupation = parts[3].strip()
        zip_code = parts[4].strip()

        return user_id, gender, age, occupation, zip_code

    except:
        return None

# BƯỚC 1: Tạo map UserID -> Gender

user_gender_rdd = users_rdd \
    .map(parse_user) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], x[1]))

# BƯỚC 2: Join với ratings để thêm thông tin giới tính

all_ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

# UserID -> (MovieID, Rating)
user_rating_rdd = all_ratings_rdd \
    .map(parse_rating) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], (x[1], x[2])))

# UserID -> ((MovieID, Rating), Gender)
rating_with_gender_rdd = user_rating_rdd.join(user_gender_rdd)

# BƯỚC 3: Tính trung bình rating cho mỗi phim theo từng giới tính

# (MovieID, Gender) -> (Rating, 1)
movie_gender_rating_rdd = rating_with_gender_rdd.map(
    lambda x: (
        (x[1][0][0], x[1][1]),      # (MovieID, Gender)
        (x[1][0][1], 1)             # (Rating, 1)
    )
)

# (MovieID, Gender) -> (TotalRating, Count)
movie_gender_sum_count_rdd = movie_gender_rating_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# (MovieID, Gender) -> (AvgRating, Count)
movie_gender_avg_count_rdd = movie_gender_sum_count_rdd.mapValues(
    lambda v: (v[0] / v[1], v[1])
)

# GẮN TÊN PHIM ĐỂ KẾT QUẢ DỄ ĐỌC

movie_title_rdd = movies_rdd \
    .map(parse_movie) \
    .filter(lambda x: x is not None)

# Đưa key về MovieID để join với title
movie_id_gender_avg_rdd = movie_gender_avg_count_rdd.map(
    lambda x: (
        x[0][0],                    # MovieID
        (x[0][1], x[1])             # (Gender, (AvgRating, Count))
    )
)

# MovieID -> ((Gender, (AvgRating, Count)), Title)
final_rdd = movie_id_gender_avg_rdd.join(movie_title_rdd)

# Format kết quả:
# MovieID, Title, Gender, AvgRating, Count
result_rdd = final_rdd.map(
    lambda x: (
        x[0],
        x[1][1],
        x[1][0][0],
        x[1][0][1][0],
        x[1][0][1][1]
    )
)

sorted_result_rdd = result_rdd.sortBy(
    lambda x: (x[0], x[2])
)

# KẾT XUẤT VÀ GHI FILE
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
output_log_path = os.path.join(OUTPUT_DIR, "Bai3.txt")
print("\nĐang xử lý và ghi kết quả, vui lòng đợi...\n")

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("BÀI 3: PHÂN TÍCH ĐÁNH GIÁ THEO GIỚI TÍNH\n")
    f.write("=" * 100 + "\n\n")

    f.write("--- ĐIỂM TRUNG BÌNH CỦA MỖI PHIM THEO GIỚI TÍNH ---\n")
    f.write(f"{'MovieID':<10} {'Title':<60} {'Gender':<8} {'AvgRating':>10} {'Count':>8}\n")
    f.write("-" * 100 + "\n")

    printed = 0

    for movie_id, title, gender, avg, count in sorted_result_rdd.toLocalIterator():
        line = f"{movie_id:<10} {title:<60} {gender:<8} {avg:>10.2f} {count:>8}"
        f.write(line + "\n")

        if printed < 20:
            print(line)
            printed += 1

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

sc.stop()