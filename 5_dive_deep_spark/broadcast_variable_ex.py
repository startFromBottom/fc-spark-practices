from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType

category_dict = {
    "10001": "도서",
    "10002": "가구",
    "10003": "전자기기",
}


def get_category_name(code: str) -> str:
    return bd_data.value.get(code)


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[3]") \
        .appName("broadcast_variable_ex") \
        .getOrCreate()

    bd_data = ss.sparkContext.broadcast(category_dict)

    data_list = [
        ("1", "10001", "2023-03-01", "12000", "10"),
        ("2", "10003", "2023-03-02", "225000", "1"),
        ("3", "10001", "2023-03-03", "20000", "3"),
        ("4", "10003", "2023-03-04", "80000", "1"),
        ("5", "10002", "2023-03-05", "90000", "2"),
    ]

    df = ss.createDataFrame(data_list) \
        .toDF("id", "category_code", "start_date", "price", "inventory_cnt")

    ss.udf.register("get_category_name", get_category_name, StringType())

    df.withColumn("category_name", expr("get_category_name(category_code)")) \
        .show()

    # while True:
    #     pass
