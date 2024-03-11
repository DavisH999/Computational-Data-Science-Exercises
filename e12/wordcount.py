import sys
import string
import re

assert sys.version_info >= (3, 8)  # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types

word_break_re = r'[%s\s]+' % (re.escape(string.punctuation),)  # regex that matches spaces and/or punctuation


def main(in_directory, out_directory):
    wordcount = spark.read.text(in_directory)
    # lower case
    wordcount = wordcount.withColumn('value', functions.lower(wordcount['value']))
    # re
    wordcount = wordcount.withColumn('list', functions.split(wordcount['value'], word_break_re, -1))
    # explode
    wordcount = wordcount.withColumn('explode', functions.explode(wordcount['list']))
    # drop cols
    wordcount = wordcount.drop('list', 'value')
    # filter ''
    wordcount = wordcount.filter(wordcount['explode'] != '')
    # count
    wordcount = wordcount.groupby('explode').agg({'*': 'count'})
    # rename cols
    wordcount = wordcount.withColumnsRenamed({'explode': 'word', 'count(1)': 'count'})
    # sort
    wordcount = wordcount.sort(wordcount['count'].desc(), wordcount['word'])
    # write to files
    wordcount.write.csv(out_directory, header=False)


if __name__ == '__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    spark = SparkSession.builder.appName('wordcount').getOrCreate()
    assert spark.version >= '3.2'  # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory, out_directory)
