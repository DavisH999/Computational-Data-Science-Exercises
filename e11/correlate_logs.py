from math import sqrt
import sys

assert sys.version_info >= (3, 8)  # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types, Row
import re

line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred.
    Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        return Row(host_name=m.group(1), bytes_transferred=int(m.group(2)))
    else:
        return None


def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory)
    log_rdd = log_lines.map(line_to_row).filter(not_none)
    return log_rdd


def compute_r(n, sum_Xi, sum_Yi, sum_Xi_sqrt, sum_Yi_sqrt, sum_XiYi):
    try:
        numerator = n * sum_XiYi - sum_Xi * sum_Yi
        denominator = sqrt(n * sum_Xi_sqrt - sum_Xi ** 2) * sqrt(n * sum_Yi_sqrt - sum_Yi ** 2)
        r = numerator / denominator
        return r
    except ZeroDivisionError:
        return None


def main(in_directory):
    logs = spark.createDataFrame(create_row_rdd(in_directory), schema='host_name:string, bytes_transferred:int')
    # (count_requests, sum_request_bytes) = (Xi, Yi)
    grouped_logs = logs.groupBy('host_name').agg((functions.count('*')).alias('Xi'),
                                                 functions.sum('bytes_transferred').alias('Yi'))
    grouped_logs = grouped_logs.withColumns({'Xi^2': functions.pow('Xi', 2),
                                             'Yi^2': functions.pow('Yi', 2),
                                             'XiYi': functions.try_multiply(grouped_logs['Xi'], grouped_logs['Yi']),
                                             '1': functions.lit(1)})
    n = grouped_logs.agg(functions.sum('1')).first()[0]
    sum_Xi = grouped_logs.agg(functions.sum('Xi')).first()[0]
    sum_Yi = grouped_logs.agg(functions.sum('Yi')).first()[0]
    sum_Xi_sqrt = grouped_logs.agg(functions.sum('Xi^2')).first()[0]
    sum_Yi_sqrt = grouped_logs.agg(functions.sum('Yi^2')).first()[0]
    sum_XiYi = grouped_logs.agg(functions.sum('XiYi')).first()[0]
    r = compute_r(n, sum_Xi, sum_Yi, sum_Xi_sqrt, sum_Yi_sqrt, sum_XiYi)
    print(f"r = {r}\nr^2 = {r * r}")
    # Built-in function should get the same results.
    # print(grouped_logs.corr('Xi', 'Yi'))


if __name__ == '__main__':
    in_directory = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.2'  # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory)
