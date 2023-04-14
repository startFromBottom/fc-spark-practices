import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[1]") \
        .appName("local-test") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    # return spark를 하지 않고 yield를 하는 이유?
    # https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#yield-fixtures-recommended
    yield spark
    spark.stop()
