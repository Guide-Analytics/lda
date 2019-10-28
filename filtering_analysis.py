##

# LDA Analysis Report
# 'filtering_analysis'
# GIDE INC 2019

# Filtering analysis contains several checks in the review data. It will involve lowercase, check alphanumeric,
# stopwords etc.

# No documentation for this one as this will not be modified unless granted so
##

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
import random

import sys
sys.path.append('/Users/michaelbrockli/PycharmProjects/lda_analysis3.0/venv/lib/python3.7/site-packages')
sys.path.append('/Users/michaelbrockli/PycharmProjects/lda_analysis3.0/venv/lib/python3.7')

import re
import string
import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

from nltk import sent_tokenize
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


class FilterCheck():

    def remove_nuke(self, data):

        '''

        :return:
        '''
        data = self.lowercase(data)
        data = self.remove_punctuation(data)
        data = self.remove_whitespace(data)
        data = self.remove_emails(data)
        data = self.remove_line_char(data)
        data = self.remove_single_quotes(data)
        data = self.remove_weirdness(data)
        data = self.remove_eclipses(data)
        data = self.check_stopwords(data)
        data = self.word_length(data)
        #self.no_review(data)

        return data

    def word_tokenize(self, data):

        self.data = word_tokenize(data)

        return self.data

    def check_stopwords(self, data):

        stop_words = stopwords.words("english")

        with open('stopwords.txt', 'rU') as f:
            for line in f:
                stop_words.append(line.strip())

        try:
            tokenize_data = word_tokenize(data)
            data = ' '.join([x for x in tokenize_data if x not in stop_words])

        except:
            pass

        return data

    def word_length(self, data):

        try:
            tokenize_data = word_tokenize(data)
            data = ' '.join([x for x in tokenize_data if len(x) > 3])
        except:
            pass

        return data

    def lowercase(self, data):

        '''

        :return:
        '''

        try:
            tokenize_data = word_tokenize(data)
            data = ' '.join([x.lower() for x in tokenize_data])
        except AttributeError:

            decode_data = data.decode('utf-8').lower()
            tokenize_data = word_tokenize(decode_data)
            data = ' '.join(tokenize_data)

        return data

    def remove_whitespace(self, data):

        '''

        :return:
        '''

        try:
            data = re.sub(r"^\s+|\s+$", "", data)
        except:
            self.remove_weirdness()
        return data

    def remove_weirdness(self, data):

        '''

        :return:
        '''

        try:
            emoji = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
            data = emoji.sub(r'', data)
        except:
            pass
        return data

    def remove_emails(self, data):

        '''
        :purpose: doesn't really work - only removing @'string' until hits whitespace
        :return:
        '''

        try:
            data = re.sub("\S@\\S*\\s?", '', data) #for sent in data
        except:
            self.remove_weirdness()

        return data

    def remove_line_char(self, data):

        try:
            data = re.sub('\\s+', ' ', data) #for sent in data
        except:
            self.remove_weirdness()
        return data

    def remove_single_quotes(self, data):

        try:
            data = re.sub("\'", "", data)
        except:
            pass
        return data

    def remove_punctuation(self, data):

        try:
            data = data.translate(str.maketrans('', '', string.punctuation))
        except AttributeError:
            pass
        return data

    def remove_eclipses(self, data):

        try:
            data = data.strip('…')
        except AttributeError:
           pass
        return data


def text_cleaner(text):

    text = str(text)
    rules = [
       {r'>\s+': u'>'}, # removes spaces after a tag opens or closes
       {r'\s+': u' '}, # replace cons. spaces
       {r'\s*<br\s*/?>\s*': u'\n'}, # new lines after a <br>
       {r'</(div)\s*>\s*': u'\n'},
       {r'</(p|h\d)\s*>\s*': u'\n\n'}, # newline after </p> and </div> and <h1/>
       {r'<head>.*<\s*(/head|body)[^>]*>': u''}, # remove <head> to </head>
       {r'<a\s+href="([^"]+)"[^>]*>.*</a>': r'\1'}, # show links instead of texts
       {r'[ \t]*<[^<]*?/?>': u''}, # remove reamining tags
       {r'^\s+': u''} # remove spaces beginning]}
    ]
    for rule in rules:
        for (k, v) in rule.items():
            regex = re.compile(k)
            text = regex.sub(v, text)
    text = text.rstrip()

    return text.lower()


def dfZipWithIndex (df, offset = 1, colName="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = StructType(
                    [StructField(colName, LongType(),True)]        # new added field in front
                    + df.schema.fields                            # previous schema
                )

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda row: ([row[1] + offset] + list(row[0])))

    return spark.createDataFrame(new_rdd, new_schema)