from numpy import array
from numpy import asarray
from pandas import DataFrame
from pandas.io import gbq
from scipy.cluster.vq import vq, kmeans, whiten

# Query to create  the TF-IDF matrix. This is the same
# query that we used in the classifying shakespeare example
# in R, and relies on having already created shakespeare_tfidf table.
query = """
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
    GROUP BY word
    """

# Actually run the query and save the results in a pandas data frame.
data_frame = gbq.read_gbq(query)

# Create a copy of the data frame that we can modify.
feature_frame = data_frame.copy()
# Remove the 'word' column, since the word name isn't going to be used in
# our clustering algorithm.
del feature_frame['word']

# Turn the features_frame data frame into a 2d array that can
# be processsed by the k means algorithm. We need to transpose the
# matrix since we want each play to be a row, not a column.
features = asarray(feature_frame.T)

# Run the k-means clustering. Save the "code book", which is the location
# of the k centroids (i.e. the locations of the clusters).
codes, _ = kmeans(features, 2)
# Assigne each of the features (i.e. plays) to one of the clusters, 
# based on whichever one is the closest.
assignments, _ = vq(features, codes)

# Match up the assignments with the play names. The result will be
# a data frame with two columns: 'play' and 'cluster'. Sort by 'cluster'
# so that it is easier to see which plays have a particular cluster id.
results = {
    'play' : array(data_frame.columns.values[1:]),
    'cluster' : assignments}
result_frame = DataFrame.from_dict(d3).sort(['cluster', 'play'])

# Voila! We should notice that one of the clusters has accurately picked out
# all of the histories.

