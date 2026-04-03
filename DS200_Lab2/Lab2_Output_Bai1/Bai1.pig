data = LOAD '/lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
    );

-- Stopwords 
stopwords = LOAD '/lab2/stopwords.txt' USING PigStorage('\n') 
AS (word: chararray);

--1.Lower case
lowercased = FOREACH data GENERATE 
    id, 
    LOWER(review) AS review;

--2.Remove punctuation
cleaned = FOREACH lowercased GENERATE 
    id, 
    REPLACE(
        REPLACE(review, '\\b\\p{N}+\\p{L}*\\b', ''),   -- xoá số + đơn vị
        '[^\\p{L}\\s]', ''                             -- xoá punctuation
    ) AS review;

--3.Tokenize
tokenized = FOREACH cleaned GENERATE 
    id, 
    FLATTEN(TOKENIZE(review)) AS word;

--4.Remove stopwords
joined = JOIN tokenized BY word LEFT OUTER, stopwords BY word;

filtered = FILTER joined BY stopwords::word IS NULL;

-- Kết quả cuối
result = FOREACH filtered GENERATE tokenized::id, tokenized::word;

preview_result = LIMIT result 20;
DUMP preview_result;

STORE result INTO '/home/h26ng/output/bai1'
USING PigStorage(',');