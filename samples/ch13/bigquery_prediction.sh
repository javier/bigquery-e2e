#!/usr/bin/bash -v
#
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.
#
# Bash commands to set up the R example bigquery_prediction.r.
# You can run this via
# bash bigquery_prediction.sh

QUERY="
SELECT s1.word AS word, 
  10000 * s1.word_count/s2.total_words AS tfidf,
  s1.corpus as corpus 
FROM (
SELECT word, word_count, corpus 
FROM [publicdata:samples.shakespeare] 
WHERE word IN (
  SELECT word
  FROM (
    SELECT word, COUNT(*) AS corpus_count
    FROM [publicdata:samples.shakespeare]
    GROUP BY word
    HAVING corpus_count > 1 AND corpus_count < 36
))) s1
JOIN (
  SELECT corpus, SUM(word_count) AS total_words 
  FROM [publicdata:samples.shakespeare]
  GROUP BY corpus
  ) s2 
ON s1.corpus = s2.corpus"

bq mk -d ch13
bq query --destination_table=ch13.shakespeare_tfidf "${QUERY}"
