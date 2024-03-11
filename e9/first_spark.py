import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('first Spark app').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8)  # make sure we have Python 3.8+
assert spark.version >= '3.4'  # make sure we have Spark 3.4+

schema = types.StructType([
    types.StructField('id', types.IntegerType()),
    types.StructField('x', types.FloatType()),
    types.StructField('y', types.FloatType()),
    types.StructField('z', types.FloatType()),
])


def main(in_directory, out_directory):
    # Read the data from the JSON files
    xyz = spark.read.json(in_directory, schema=schema)
    # xyz.show()
    # return

    # Create a DF with what we need: x, (soon y,) and id%10 which we'll aggregate by.
    with_bins = xyz.select(
        xyz['x'],
        # TODO: also the y values
        xyz['y'],
        (xyz['id'] % 10).alias('bin'),
    )
    # with_bins.show()
    # return

    # Aggregate by the bin number.
    grouped = with_bins.groupBy(with_bins['bin'])
    groups = grouped.agg(
        functions.sum(with_bins['x']).alias('sum'),
        # TODO: output the average y value. Hint: avg
        functions.avg(with_bins['y']),
        functions.count('*'))
    groups.show()
    # We know groups has <=10 rows, so it can safely be moved into two partitions.
    groups = groups.sort(groups['bin']).coalesce(2)
    groups.write.csv(out_directory, compression=None, mode='overwrite')


if __name__ == '__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
