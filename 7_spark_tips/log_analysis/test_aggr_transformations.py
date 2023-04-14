import pytest
from pyspark import Row
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality

from .transformations import *


@pytest.fixture(scope="session")
def log_data(spark: SparkSession) -> DataFrame:
    # 원본 데이터의 모든 컬럼을 넣을 필요는 없음.
    # unit test에 필요한 컬럼만 추가!

    data = [
        {"method": "GET", "endpoint": "/users", "latency": 10},
        {"method": "GET", "endpoint": "/users", "latency": 10},
        {"method": "POST", "endpoint": "/users", "latency": 10},
        {"method": "POST", "endpoint": "/users", "latency": 5},
        {"method": "POST", "endpoint": "/stocks", "latency": 0},
        {"method": "POST", "endpoint": "/stocks", "latency": 100},
    ]

    df = spark.createDataFrame(Row(**x) for x in data)
    df.cache()
    df.count()
    return df


def test_aggregate_latency_infos(spark: SparkSession, log_data: DataFrame):
    # given
    expected_data = [
        {"method": "GET", "endpoint": "/users", "max_latency": 10, "min_latency": 10, "mean_latency": 10.0},
        {"method": "POST", "endpoint": "/users", "max_latency": 10, "min_latency": 5, "mean_latency": 7.5},
        {"method": "POST", "endpoint": "/stocks", "max_latency": 100, "min_latency": 0, "mean_latency": 50.0},
    ]
    expected_df = spark.createDataFrame(Row(**x) for x in expected_data)

    # when
    result_df = aggregate_latency_infos(log_data)
    # then
    assert_df_equality(result_df, expected_df)
