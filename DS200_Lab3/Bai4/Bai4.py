from pyspark import SparkContext, SparkConf
import os

conf = SparkConf().setAppName("DS200_Lab3_Bai4").setMaster("local[*]")
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


def get_age_group(age):
    age_group_map = {
        1: "<18",
        18: "18-24",
        25: "25-34",
        35: "35-44",
        45: "45-49",
        50: "50-55",
        56: "56+"
    }

    if age in age_group_map:
        return age_group_map[age]
    if age < 18:
        return "<18"
    elif age <= 24:
        return "18-24"
    elif age <= 34:
        return "25-34"
    elif age <= 44:
        return "35-44"
    elif age <= 49:
        return "45-49"
    elif age <= 55:
        return "50-55"
    else:
        return "56+"


def shorten_title(title, width=35):
    """
    Cắt tên phim để output không quá dài.
    """
    if len(title) <= width:
        return title
    return title[:width]


def format_age_cell(age_group, age_dict):
    """
    age_dict có dạng:
    {
        "25-34": (avg_rating, count),
        "35-44": (avg_rating, count),
        ...
    }
    """
    if age_group not in age_dict:
        return f"{age_group}: N/A"

    avg, count = age_dict[age_group]
    return f"{age_group}: {avg:.2f} ({count} votes)"

# BƯỚC 1: Tạo map UserID -> Age Group

user_age_group_rdd = users_rdd \
    .map(parse_user) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], get_age_group(x[2])))

# UserID -> AgeGroup

# BƯỚC 2: Join với ratings để thêm nhóm tuổi

all_ratings_rdd = ratings_1_rdd.union(ratings_2_rdd)

# UserID -> (MovieID, Rating)
user_rating_rdd = all_ratings_rdd \
    .map(parse_rating) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], (x[1], x[2])))

# UserID -> ((MovieID, Rating), AgeGroup)
rating_with_age_group_rdd = user_rating_rdd.join(user_age_group_rdd)

# BƯỚC 3: Tính trung bình điểm đánh giá theo nhóm tuổi

# (MovieID, AgeGroup) -> (Rating, 1)
movie_age_rating_rdd = rating_with_age_group_rdd.map(
    lambda x: (
        (x[1][0][0], x[1][1]),      # (MovieID, AgeGroup)
        (x[1][0][1], 1)             # (Rating, 1)
    )
)

# (MovieID, AgeGroup) -> (TotalRating, Count)
movie_age_sum_count_rdd = movie_age_rating_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# (MovieID, AgeGroup) -> (AvgRating, Count)
movie_age_avg_count_rdd = movie_age_sum_count_rdd.mapValues(
    lambda v: (v[0] / v[1], v[1])
)


# ============================================================
# FORMAT KẾT QUẢ THEO DẠNG MỖI PHIM 1 DÒNG
# ============================================================

age_groups = ["<18", "18-24", "25-34", "35-44", "45-49", "50-55", "56+"]

movie_title_rdd = movies_rdd \
    .map(parse_movie) \
    .filter(lambda x: x is not None)

# Từ:
# (MovieID, AgeGroup) -> (AvgRating, Count)
# chuyển thành:
# MovieID -> (AgeGroup, AvgRating, Count)
movie_age_row_rdd = movie_age_avg_count_rdd.map(
    lambda x: (
        x[0][0],                    # MovieID
        (x[0][1], x[1][0], x[1][1]) # (AgeGroup, AvgRating, Count)
    )
)

# Gom các nhóm tuổi của cùng một phim lại
# MovieID -> [(AgeGroup, AvgRating, Count), ...]
movie_age_grouped_rdd = movie_age_row_rdd.groupByKey().mapValues(list)

# Join với title
# MovieID -> (Title, [(AgeGroup, AvgRating, Count), ...])
final_rdd = movie_title_rdd.join(movie_age_grouped_rdd)

# Sắp xếp theo MovieID
sorted_final_rdd = final_rdd.sortBy(lambda x: x[0])


# ============================================================
# KẾT XUẤT VÀ GHI FILE
# ============================================================
output_log_path = "Bai4.txt"

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("ĐIỂM TRUNG BÌNH PHIM THEO NHÓM TUỔI\n")

    printed = 0

    for movie_id, (title, age_rows) in sorted_final_rdd.toLocalIterator():
        # Chuyển list thành dict để tra nhanh từng nhóm tuổi
        age_dict = {}
        for age_group, avg, count in age_rows:
            age_dict[age_group] = (avg, count)

        title_display = shorten_title(title, 35)

        line = f"ID: {movie_id:<4} | {title_display:<35}"

        for age_group in age_groups:
            cell = format_age_cell(age_group, age_dict)
            line += f" | {cell:<21}"

        f.write(line + "\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

sc.stop()