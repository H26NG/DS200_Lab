-- LOAD DATA
data = LOAD '/home/h26ng/output/bai1' USING PigStorage(',') AS (
    id: int,
    word: chararray
);

hotel_review = LOAD '/lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- 1. Thống kê tần số từ (xuất hiện > 500 lần)
word_freq_raw = FOREACH (GROUP data BY word) GENERATE 
    group AS word, 
    COUNT(data) AS count;

word_over_500 = FILTER word_freq_raw BY count > 500;
word_over_500_sorted = ORDER word_over_500 BY count DESC;

preview_data = LIMIT word_over_500_sorted 20;

DUMP word_over_500_sorted;
-- 2. Thống kê số bình luận theo category
category_counts = FOREACH (GROUP hotel_review BY category) GENERATE 
    group AS category, 
    COUNT(hotel_review) AS count;

category_sorted = ORDER category_counts BY count DESC;
DUMP category_sorted;

-- 3. Thống kê số bình luận theo aspect
group_aspect = GROUP hotel_review BY aspect;
count_aspect = FOREACH group_aspect GENERATE 
    group AS aspect, 
    COUNT(hotel_review) AS count;

aspect_sorted = ORDER count_aspect BY count DESC;
DUMP aspect_sorted;

-- STORE RESULTS
STORE word_over_500_sorted INTO '/home/h26ng/output/bai2/word_freq' USING PigStorage(',');
STORE category_sorted INTO '/home/h26ng/output/bai2/category_count' USING PigStorage(',');
STORE aspect_sorted INTO '/home/h26ng/output/bai2/aspect_count' USING PigStorage(',');