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

--LỌC CẢM XÚC TRƯỚC KHI JOIN

pos_data = FILTER hotel_review BY sentiment == 'positive';
neg_data = FILTER hotel_review BY sentiment == 'negative';

--XỬ LÝ NHÓM TÍCH CỰC (Top 5 từ theo Category)
pos_joined = JOIN data BY id, pos_data BY id;
pos_extract = FOREACH pos_joined GENERATE pos_data::category AS category, data::word AS word;

-- Đếm tần suất mỗi từ trong từng category
pos_word_group = GROUP pos_extract BY (category, word);
pos_word_freq = FOREACH pos_word_group GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(pos_extract) AS freq;

-- Nhóm lại theo Category và lấy Top 5
pos_cat_group = GROUP pos_word_freq BY category;
pos_top5 = FOREACH pos_cat_group {
    sorted_pos = ORDER pos_word_freq BY freq DESC;
    top_5_pos = LIMIT sorted_pos 5;
    GENERATE group AS category, top_5_pos;
};

neg_joined = JOIN data BY id, neg_data BY id;

neg_extract = FOREACH neg_joined GENERATE neg_data::category AS category, data::word AS word;

neg_word_group = GROUP neg_extract BY (category, word);
neg_word_freq = FOREACH neg_word_group GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(neg_extract) AS freq;

neg_cat_group = GROUP neg_word_freq BY category;
neg_top5 = FOREACH neg_cat_group {
    sorted_neg = ORDER neg_word_freq BY freq DESC;
    top_5_neg = LIMIT sorted_neg 5;
    GENERATE group AS category, top_5_neg;
};

STORE pos_top5 INTO '/home/h26ng/output/bai4/top5_positive' USING PigStorage(',');
STORE neg_top5 INTO '/home/h26ng/output/bai4/top5_negative' USING PigStorage(',');