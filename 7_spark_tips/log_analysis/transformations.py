from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_by_status_code_400_and_users_endpoint(df: DataFrame) -> DataFrame:
    return df.filter((df.status_code == "400") & (df.endpoint == "/users"))


def aggregate_latency_infos(df: DataFrame) -> DataFrame:
    group_cols = ["method", "endpoint"]

    return df.groupby(group_cols) \
        .agg(F.max("latency").alias("max_latency"),
             F.min("latency").alias("min_latency"),
             F.mean("latency").alias("mean_latency"))
