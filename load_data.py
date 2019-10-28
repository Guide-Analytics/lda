##
# LDA Analysis Report
# 'load_data'
#
# GIDE INC 2019
##

import random
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import regexp_replace, udf
from pyspark.sql.types import StringType
from filtering_analysis import text_cleaner

sc = SparkContext(appName="LDA 3.0 Test 1", master="local[*]", conf=SparkConf().set('spark.ui.port', random.randrange(4000, 5000)))
sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName("LDA 3.0 Test 2").master("local[2]")\
                     .config('spark.ui.port', random.randrange(4000, 5000)).getOrCreate()
path = 'data/GOOGLE_REVIEWS.csv'


# load_dataset_and_set_views:
# Purpose: reading the data from csv through SPARK
# Input; [String] pathR
# Output: [Spark DataFrame] rev_data
def load_dataset_and_set_views(pathR=path):

    rev_data_raw = spark.read.csv(pathR, mode="PERMISSIVE", header='true', sep=',', inferSchema=True,
                                  multiLine=True, quote='"', escape='"')

    rev_data_raw = rev_data_raw.withColumn('Review Text', regexp_replace('Review Text', '"', ''))

    rev_data = rev_data_raw.toDF("GOOGLE_REVIEWS ID", "Business Rating", "Business Reviews", "Source URL",
                                 "Business Name", "Author Name", "Local Guide", "Review Text", "Review Rating",
                                 "Review Date", "Author URL")

    udf_text_cleaner = udf(text_cleaner, StringType())

    rev_data = rev_data.withColumn('Review Text', udf_text_cleaner('Review Text'))
    rev_data.createOrReplaceTempView("rev_data")

    return rev_data
