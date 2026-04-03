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

--NỐI (JOIN) VÀ TRÍCH XUẤT DỮ LIỆU
joined_data = JOIN data BY id, hotel_review BY id;

extracted_data = FOREACH joined_data GENERATE hotel_review::category AS category, data::word AS word;

--ĐẾM TẦN SUẤT TỪ THEO TỪNG CATEGORY
word_group = GROUP extracted_data BY (category, word);

word_freq = FOREACH word_group GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(extracted_data) AS freq;

--TÌM TOP 5
cat_group = GROUP word_freq BY category;

top5_words = FOREACH cat_group {
    sorted_words = ORDER word_freq BY freq DESC;
    top_5 = LIMIT sorted_words 5;
    GENERATE group AS category, top_5;
};

STORE top5_words INTO '/home/h26ng/output/bai5' USING PigStorage(',');