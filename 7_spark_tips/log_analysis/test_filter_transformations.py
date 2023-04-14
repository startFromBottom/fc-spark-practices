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
        {"ip": "192.168.100.70", "status_code": "200", "endpoint": "/users"},
        {"ip": "192.168.100.71", "status_code": "400", "endpoint": "/stocks"},
        {"ip": "90.87.227.59", "status_code": "500", "endpoint": "/abcd"},
        {"ip": "100.232.50.8", "status_code": "400", "endpoint": "/users"},
    ]

    df = spark.createDataFrame(Row(**x) for x in data)
    df.cache()
    df.count()
    return df


def test_filter_by_status_code_400_and_users_endpoint(spark: SparkSession, log_data: DataFrame):
    # given
    expected_data = [{"ip": "100.232.50.8", "status_code": "400", "endpoint": "/users"}]
    expected_df = spark.createDataFrame(Row(**x) for x in expected_data)

    # when
    result_df = filter_by_status_code_400_and_users_endpoint(log_data)

    # then
    assert_df_equality(result_df, expected_df)

