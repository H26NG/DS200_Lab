from pyspark import SparkContext, SparkConf
import os

conf = SparkConf().setAppName("DS200_Lab3_Bai5").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

print("SparkContext đã được khởi tạo thành công.")
print(f"Spark version: {sc.version}")

DATA_DIR = "."

ratings_1_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_1 (1).txt"))
ratings_2_rdd = sc.textFile(os.path.join(DATA_DIR, "ratings_2 (1).txt"))
users_rdd = sc.textFile(os.path.join(DATA_DIR, "users (1).txt"))
occupation_rdd = sc.textFile(os.path.join(DATA_DIR, "occupation.txt"))


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


def parse_occupation(line):
    """
    occupation.txt thường có dạng:
    OccupationID,OccupationName

    Ví dụ:
    0,other
    1,academic/educator
    """
    try:
        line = line.strip()

        if "::" in line:
            parts = line.split("::", 1)
        elif "\t" in line:
            parts = line.split("\t", 1)
        else:
            parts = line.split(",", 1)

        occupation_id = parts[0].strip()
        occupation_name = parts[1].strip()

        return occupation_id, occupation_name

    except:
        return None

occupation_id_to_name = occupation_rdd \
    .map(parse_occupation) \
    .filter(lambda x: x is not None) \
    .collectAsMap()

occupation_bc = sc.broadcast(occupation_id_to_name)

# Bước 1: Tạo dictionary từ users.txt với mapping UserID -> Occupation

user_occupation_rdd = users_rdd \
    .map(parse_user) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (
        x[0],
        occupation_bc.value.get(x[3], f"OccupationID_{x[3]}")
    ))

# user_occupation_rdd:
# UserID -> Occupation

# Bước 2: Với mỗi rating, gán thông tin Occupation theo UserID
all_ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

# UserID -> Rating
user_rating_rdd = all_ratings_rdd \
    .map(parse_rating) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], x[2]))

# UserID -> (Rating, Occupation)
rating_with_occupation_rdd = user_rating_rdd.join(user_occupation_rdd)


# Bước 3: key-value với key là Occupation và value là (rating, 1)
occupation_rating_rdd = rating_with_occupation_rdd.map(
    lambda x: (
        x[1][1],        # Occupation
        (x[1][0], 1)    # (Rating, 1)
    )
)

# occupation_rating_rdd:
# Occupation -> (Rating, 1)

# Bước 4: Reduce để tính tổng điểm và số lượt cho mỗi Occupation,sau đó tính trung bình rating

occupation_sum_count_rdd = occupation_rating_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

occupation_avg_count_rdd = occupation_sum_count_rdd.mapValues(
    lambda v: (v[0] / v[1], v[1])
)

# Sắp xếp:
# 1. AvgRating giảm dần
# 2. Count giảm dần
# 3. Tên occupation tăng dần
sorted_result_rdd = occupation_avg_count_rdd.sortBy(
    lambda x: (-x[1][0], -x[1][1], x[0])
)

output_log_path = "Bai5.txt"
print("\nĐang xử lý và ghi kết quả, vui lòng đợi...\n")

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("BÀI 5: PHÂN TÍCH ĐÁNH GIÁ THEO OCCUPATION\n")
    f.write("=" * 80 + "\n\n")

    f.write("--- ĐIỂM TRUNG BÌNH VÀ TỔNG SỐ LƯỢT ĐÁNH GIÁ THEO OCCUPATION ---\n")
    f.write(f"{'Occupation':<35} {'AvgRating':>12} {'Count':>10}\n")
    f.write("-" * 80 + "\n")

    for occupation, (avg, count) in sorted_result_rdd.toLocalIterator():
        line = f"{occupation:<35} {avg:>12.2f} {count:>10}"
        print(line)
        f.write(line + "\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

sc.stop()