import sys

assert sys.version_info >= (3, 8)  # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    # types.StructField('year', types.IntegerType()),
    # types.StructField('month', types.IntegerType()),
])


def str_func(a: str) -> int:
    return 7


def main(in_directory, out_directory):
    comments = spark.read.json(in_directory, schema=comments_schema)

    # new = comments.select('score', 'id')
    # new = new.withColumn('new_core', new['score']*100)
    # new = new.groupBy('score').agg(functions.sum('new_core').alias('sum'))
    # new = new.collect()
    # print(new[0:5])

    grouped_comments = comments.groupBy('subreddit')
    agg_comments = grouped_comments.agg(functions.avg('score').alias('avg_score'))
    filtered_comments = agg_comments.filter(agg_comments['avg_score'] > 0)
    joined_comments = filtered_comments.hint('broadcast').join(comments, on='subreddit')
    joined_comments = joined_comments.withColumn('relative_score',
                                                 joined_comments['score'] / joined_comments['avg_score'])
    # use cache here due to DataFrame (joined_comments) that is used more than once in the following part.
    joined_comments = joined_comments.cache()
    grouped_again_comments = joined_comments.groupBy('subreddit').agg(
        functions.max('relative_score').alias('max_relative_score'))
    joined_again_comments = grouped_again_comments.hint('broadcast').join(joined_comments, on='subreddit').filter(
        grouped_again_comments['max_relative_score'] == joined_comments['relative_score'])
    selected_comments = joined_again_comments.select('subreddit', 'author', 'max_relative_score').withColumnRenamed(
        'max_relative_score', 'rel_score').orderBy('subreddit')
    # selected_comments.show()
    selected_comments.write.json(out_directory, mode='overwrite')


if __name__ == '__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    spark = SparkSession.builder.appName('Reddit Relative Scores').getOrCreate()
    assert spark.version >= '3.2'  # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory, out_directory)