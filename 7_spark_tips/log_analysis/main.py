from pyspark.sql import SparkSession
from pyspark.sql.types import *

from transformations import *

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log dataframe ex") \
        .getOrCreate()

    # define schema
    schema = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),  # 단위 : milliseconds
    ])

    df = ss.read.schema(schema).csv("data/log.csv")

    filter_df = filter_by_status_code_400_and_users_endpoint(df)
    result = aggregate_latency_infos(filter_df)

    result.show()
