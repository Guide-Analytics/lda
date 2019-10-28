##
# LDA Analysis Report
# 'lda_analysis'
#
# GIDE INC 2019
##

import os
import sys
# from nltk.corpus import stopwords
from filtering_analysis import FilterCheck
from load_data import load_dataset_and_set_views, sqlContext, sc
from pyspark.ml.feature import CountVectorizer, IDF

# This code is for a weird situation where libraries/APIs of Pyspark ML is not importing numpy
# correctly into the program. We need
os.environ['PYSPARK_PYTHON'] = sys.executable

# stuff we'll need for building the model
from pyspark.mllib.linalg import Vectors as MLlibVectors
from pyspark.mllib.clustering import LDA as MLlibLDA

rev_data = load_dataset_and_set_views()


# reviews_tokens:
# Purpose: Tokenize the reviews and filter out necessary punctuations and whitelines
# Input: none
# Output: [String] reviews
def reviews_tokens():

    reviews = rev_data.rdd.map(lambda x : x['Review Text']).filter(lambda x : x is not None)\
                          .filter(lambda x: x is not u'')

    data = FilterCheck()

    reviews = reviews.map(data.remove_nuke).zipWithIndex()\
                     .map(lambda x: (data.word_tokenize(x[0]), x[1] + 1))

    return reviews

# tf_idf_call:
# Purpose: Call on tf_idf - term frequency–inverse document frequency,
# and adjust number of topics and iterations for unsupervised training model on the reviews
# Input: [Int] num_topics, [Int] max_iter
# Output: [Spark Model] lda_model, [Spark Feature Model] cvmodel
def tf_idf_call(num_topics = 4, max_iter=100):

    reviews = reviews_tokens()
    df_txts = sqlContext.createDataFrame(reviews, ["list_of_words", "index"])

    # TF
    cv = CountVectorizer(inputCol="list_of_words", outputCol="raw_features", vocabSize=5000, minDF=10.0)
    cvmodel = cv.fit(df_txts)

    result_cv = cvmodel.transform(df_txts)

    # IDF
    idf = IDF(inputCol="raw_features", outputCol="features")
    idfmodel = idf.fit(result_cv)
    result_tfidf = idfmodel.transform(result_cv)

    result_list = result_tfidf[["index", "features"]].rdd.mapValues(MLlibVectors.fromML)
    result_list = result_list.map(list)

    doc_conc = int(50 / num_topics)

    lda_model = MLlibLDA.train(result_list, k=num_topics, maxIterations=max_iter, optimizer='em', checkpointInterval=10)

    return lda_model, cvmodel


# topic_render:
# Purpose: Extract all topics from [vocabArray] based on their index positions [wordNumbers]
# Input: [List] topic, [Int] wordNumbers, [Dict] vocabArray
# Output: [List] result
def topic_render(topic, wordNumbers, vocabArray):

    terms = topic[0]
    result = []
    for i in range(wordNumbers):
        term = vocabArray[terms[i]]
        result.append(term)
    return result


# topic_output:
# Purpose: Outputting the topics
# Input: [Int] terms_topic
# Output: (testing purposes) vocabArray, [List] topics, [List] main_topics
def topic_output(terms_topic=5):

    lda_model, cvmodel = tf_idf_call()

    save_file = lda_model.topicsMatrix()

    vocabArray = cvmodel.vocabulary

    # Set a new RDD
    topic_indices = sc.parallelize(lda_model.describeTopics(maxTermsPerTopic=terms_topic))
    topics = topic_indices.collect()

    # Set topics
    topics_final = topic_indices.map(lambda topic: topic_render(topic, terms_topic, vocabArray)).collect()

    main_topics = {}

    for topic in range(len(topics_final)):
        lst_terms = []
        for term in topics_final[topic]:
            lst_terms.append(term)
        main_topics.update({topic: lst_terms})

    return vocabArray, topics, main_topics
