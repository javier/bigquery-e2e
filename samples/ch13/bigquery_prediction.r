# BigQuery End-To-End Chapter 13
# Predicting Shakespeare Sample

# Make sure reequired libraries are present.
require("Rook")
require("rjson")
require("devtools")
require("bigrquery")
require("e1071")

billing_project <- "bigquery-e2e"

# Test out authentication.
query_exec("publicdata", "samples", "SELECT 17", billing=billing_project)

# Set up our pivot query to get the TF-IDF values.
query <- "
SELECT word,
SUM(if (corpus == '1kinghenryiv', tfidf, 0)) as onekinghenryiv,
SUM(if (corpus == '1kinghenryvi', tfidf, 0)) as onekinghenryvi,
SUM(if (corpus == '2kinghenryiv', tfidf, 0)) as twokinghenryiv,
SUM(if (corpus == '2kinghenryvi', tfidf, 0)) as twokinghenryvi,
SUM(if (corpus == '3kinghenryvi', tfidf, 0)) as threekinghenryvi,
SUM(if (corpus == 'allswellthatendswell', tfidf, 0)) as allswellthatendswell,
SUM(if (corpus == 'antonyandcleopatra', tfidf, 0)) as antonyandcleopatra,
SUM(if (corpus == 'asyoulikeit', tfidf, 0)) as asyoulikeit,
SUM(if (corpus == 'comedyoferrors', tfidf, 0)) as comedyoferrors,
SUM(if (corpus == 'coriolanus', tfidf, 0)) as coriolanus,
SUM(if (corpus == 'cymbeline', tfidf, 0)) as cymbeline,
SUM(if (corpus == 'hamlet', tfidf, 0)) as hamlet,
SUM(if (corpus == 'juliuscaesar', tfidf, 0)) as juliuscaesar,
SUM(if (corpus == 'kinghenryv', tfidf, 0)) as kinghenryv,
SUM(if (corpus == 'kinghenryviii', tfidf, 0)) as kinghenryviii,
SUM(if (corpus == 'kingjohn', tfidf, 0)) as kingjohn,
SUM(if (corpus == 'kinglear', tfidf, 0)) as kinglear,
SUM(if (corpus == 'kingrichardii', tfidf, 0)) as kingrichardii,
SUM(if (corpus == 'kingrichardiii', tfidf, 0)) as kingrichardiii,
SUM(if (corpus == 'loverscomplaint', tfidf, 0)) as loverscomplaint,
SUM(if (corpus == 'loveslabourslost', tfidf, 0)) as loveslabourslost,
SUM(if (corpus == 'macbeth', tfidf, 0)) as macbeth,
SUM(if (corpus == 'measureforemeasure', tfidf, 0)) as measureforemeasure,
SUM(if (corpus == 'merchantofvenice', tfidf, 0)) as merchantofvenice,
SUM(if (corpus == 'merrywivesofwindsor', tfidf, 0)) as merrywivesofwindsor,
SUM(if (corpus == 'midsummersnightsdream', tfidf, 0)) as midsummersnightsdream,
SUM(if (corpus == 'muchadoaboutnothing', tfidf, 0)) as muchadoaboutnothing,
SUM(if (corpus == 'othello', tfidf, 0)) as othello,
SUM(if (corpus == 'periclesprinceoftyre', tfidf, 0)) as periclesprinceoftyre,
SUM(if (corpus == 'romeoandjuliet', tfidf, 0)) as romeoandjuliet,
SUM(if (corpus == 'tamingoftheshrew', tfidf, 0)) as tamingoftheshrew,
SUM(if (corpus == 'tempest', tfidf, 0)) as tempest,
SUM(if (corpus == 'timonofathens', tfidf, 0)) as timonofathens,
SUM(if (corpus == 'titusandronicus', tfidf, 0)) as titusandronicus,
SUM(if (corpus == 'troilusandcressida', tfidf, 0)) as troilusandcressida,
SUM(if (corpus == 'twelfthnight', tfidf, 0)) as twelfthnight,
SUM(if (corpus == 'twogentlemenofverona', tfidf, 0)) as twogentlemenofverona,
SUM(if (corpus == 'winterstale', tfidf, 0)) as winterstale,
FROM [bigquery-e2e:scratch.shakespeare_tfidf]
GROUP BY word"

# Run our TF-IDF query.
results <- query_exec("publicdata", "samples", query, billing=billing_project, 
    max_pages=Inf)
summary(results)

# Label the rows with the corresponding word and drop the "word" column.
rownames(results) <- results$word
results$word <- NULL

# Build the labels data frame that we'll use for training.
categories_str = "
  corpus, type
  onekinghenryiv, history
  onekinghenryvi, history
  twokinghenryiv, history
  twokinghenryvi, history
  threekinghenryvi, history
  allswellthatendswell, comedy
  antonyandcleopatra, tragedy
  asyoulikeit, comedy
  comedyoferrors, comedy
  coriolanus, tragedy
  cymbeline, tragedy
  hamlet, tragedy
  juliuscaesar, tragedy
  kinghenryv, history
  kinghenryviii, history
  kingjohn, history
  kinglear, history
  kingrichardii, history
  kingrichardiii, history
  loverscomplaint, comedy
  loveslabourslost, comedy
  macbeth, tragedy
  measureforemeasure, comedy
  merchantofvenice, comedy
  merrywivesofwindsor, comedy
  midsummersnightsdream, comedy
  muchadoaboutnothing, comedy
  othello, tragedy
  periclesprinceoftyre, history
  romeoandjuliet, tragedy
  tamingoftheshrew, comedy
  tempest, comedy
  timonofathens, tragedy
  titusandronicus, tragedy
  troilusandcressida, tragedy
  twelfthnight, comedy
  twogentlemenofverona, comedy
  winterstale, comedy
"
categories = read.csv(text=categories_str)
rownames(categories) <- categories$corpus
categories$corpus <- NULL
summary(categories)

# Train the Naive Bayesian classifier.
classifier<-naiveBayes(t(results), categories[,1])

# Use our trained model to predict the values
predictions<-predict(classifier, t(results))
predictions

# Print a table of prediction accuracy.
table(predictions, categories[,1], dnn=list('predicted','actual'))
