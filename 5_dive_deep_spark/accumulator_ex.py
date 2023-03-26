from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[3]") \
        .appName("accumulator_ex") \
        .getOrCreate()

    bad_rec = ss.sparkContext.accumulator(0)


    # udf
    def count_only_valid(visit_count: str) -> int:
        try:
            return int(visit_count)
        except ValueError:
            bad_rec.add(1)
            return 0


    ss.udf.register("count_only_valid", count_only_valid, IntegerType())

    data_list = [
        ("1", "korea", "2"),
        ("2", "japan", "3"),
        ("3", "china", "vvvv"),
        ("4", "japan", "1"),
        ("5", "india", "4"),
        ("6", "korea", "three"),
    ]

    df = ss.createDataFrame(data_list) \
        .toDF("user_id", "visit_country", "visit_count")

    # add in transformation
    df.withColumn("visit_count_valid",
                  expr("count_only_valid(visit_count)")) \
        .show()

    print(f"Bad Record Count: {bad_rec.value}")

    # while True:
    #     pass
