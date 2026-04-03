hotel_review = LOAD '/lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int, review: chararray, category: chararray, aspect: chararray, sentiment: chararray
);

-- TÌM TOP 1 TÍCH CỰC
positive_reviews = FILTER hotel_review BY sentiment == 'positive';
pos_group = GROUP positive_reviews BY aspect; 
pos_counts = FOREACH pos_group GENERATE group AS aspect, COUNT(positive_reviews) as total;

pos_all = GROUP pos_counts ALL;
pos_result = FOREACH pos_all {
    pos_sorted = ORDER pos_counts BY total DESC;
    pos_top1 = LIMIT pos_sorted 1;
    GENERATE FLATTEN(pos_top1);
};

STORE pos_result INTO '/home/h26ng/output/bai3/positive_aspect' USING PigStorage(',');

-- TÌM TOP 1 TIÊU CỰC
negative_reviews = FILTER hotel_review BY sentiment == 'negative';
neg_group = GROUP negative_reviews BY aspect;
neg_counts = FOREACH neg_group GENERATE group AS aspect, COUNT(negative_reviews) as total;

-- Áp dụng TRICK tương tự cho nhóm Tiêu cực
neg_all = GROUP neg_counts ALL;
neg_result = FOREACH neg_all {
    neg_sorted = ORDER neg_counts BY total DESC;
    neg_top1 = LIMIT neg_sorted 1;
    GENERATE FLATTEN(neg_top1);
};

STORE neg_result INTO '/home/h26ng/output/bai3/negative_aspect' USING PigStorage(',');