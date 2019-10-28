##
# LDA Analysis Report
# 'report_output'
#
# GIDE INC 2019
##

from back_to_review import reviewToKeyword, reviewToTopic, main_topics, rev_data
from pyspark.sql.functions import udf, regexp_replace
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
path = 'topics/topics.txt'


# report_output:
# Purpose: Outputting the report to CSV through Spark SQL and get results of Topic Keywords and Topics
# Input: [Spark DataFrame] rev_data
# Output: None (CSV File of the result)
def report_output(rev_data):
    with open(path, 'w+') as file:
        for topic, values in main_topics.items():
            file.write('Topic: ' + str(topic + 1))
            file.write('\n')
            file.writelines(str(values))
            file.write('\n \n')

    udf_keywords = udf(reviewToKeyword, StringType())
    udf_topics = udf(reviewToTopic, StringType())

    new_rev_data = rev_data.withColumn('Topic Keywords', udf_keywords('Review Text'))
    new_rev_data = new_rev_data.withColumn('Topics', udf_topics('Review Text'))

    # new_rev_data.show()
    new_rev_data.write.format('csv').mode('overwrite').option("header", "true").save("review_info")


report_output(rev_data)
