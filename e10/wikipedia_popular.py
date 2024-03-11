import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8)  # make sure we have Python 3.8+
assert spark.version >= '3.2'  # make sure we have Spark 3.2+


def split_str(filename):
    s1 = filename.split('/')[-1]
    s2 = s1.split('-')
    return s2[-2] + '-' + s2[-1][0] + s2[-1][1]


path_to_hour = functions.udf(lambda x: split_str(x), returnType=types.StringType())


def main(in_directory, out_directory):
    wiki_schema = types.StructType([
        types.StructField('lang', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('requested_times', types.DoubleType()),
        types.StructField('total_bytes', types.DoubleType())
    ])
    wikipedia = spark.read.csv(in_directory, sep=' ', schema=wiki_schema).withColumn('filename',
                                                                                     functions.input_file_name())
    # add one col 'hour'
    wikipedia = wikipedia.withColumn('day_hour', path_to_hour(wikipedia['filename']))
    # filter data
    wikipedia = wikipedia.filter(wikipedia['lang'] == 'en')
    wikipedia = wikipedia.filter(wikipedia['title'] != 'Main_Page')
    wikipedia = wikipedia.filter(~ wikipedia['title'].startswith('Special:'))
    # cache
    wikipedia = wikipedia.cache()
    # group data
    grouped_wikipedia = wikipedia.groupBy(wikipedia['day_hour'].alias('group_day_hour')).agg(
        functions.max(wikipedia['requested_times']).alias('max_requested_times'))
    # join data
    joined_wikipedia = grouped_wikipedia.join(wikipedia)
    # filter the joined data
    joined_wikipedia = joined_wikipedia.filter(
        joined_wikipedia['max_requested_times'] == joined_wikipedia['requested_times'])
    # select cols and sort
    joined_wikipedia = joined_wikipedia.select('group_day_hour', 'title', 'max_requested_times').orderBy('group_day_hour', 'title')
    # joined_wikipedia.show()
    # write out
    joined_wikipedia.write.csv(out_directory + '-wiki', mode='overwrite')


if __name__ == '__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
