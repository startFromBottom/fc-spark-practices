from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

"""

source data : https://github.com/databricks/LearningSparkV2/blob/master/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv

"""

# define schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log dataframe ex") \
        .getOrCreate()

    df = ss.read.schema(fire_schema).csv("data/sf-fire-calls.csv", header=True)

    # data check
    # df.printSchema()
    # print(df.count())

    # Q-1) 2018???(CallDate)??? ?????? ?????? ???????????? ?????? ??????(CallType)??? ????????????,
    # ?????? ?????? ?????? ??????.

    # 2018??? ???????????? ?????????
    df = df.withColumn("call_year",
                       f.year(f.to_timestamp("CallDate", "dd/MM/yyyy"))) \
        .filter(f.col("call_year") == "2018")

    df.select("CallType").where(f.col("CallType").isNotNull()) \
        .groupby("CallType") \
        .count().orderBy("count", ascending=False) \
        # .show(n=10, truncate=False)

    # Q-2) 2018?????? ??? ???(month)??? ?????? ??? ?????? ???, ?????? ?????? ?????? ?????? ??? ????????????.
    df.withColumn("month",
                  f.month(f.to_timestamp("CallDate", "dd/MM/yyyy"))) \
        .groupby("month") \
        .count().orderBy("count", ascending=False) \
        # .show(n=10, truncate=False)

    # Q-3) 2018?????? ?????? ?????? ????????? ????????? ?????????????????? ??????????
    df.filter(f.col("City") == "San Francisco") \
        .groupby("Address").count().orderBy("count", ascending=False) \
        # .show(n=10, truncate=False)

    # Q-4) 2018?????? ????????????????????? ?????? ?????? ??? ?????? ????????? ?????? ?????? ?????? ???????
    res = df.select("Neighborhood", "Delay") \
        .filter(f.col("call_year") == "2018").orderBy("Delay", ascending=False) \
        .take(5)
    # Q-5) 2018??? ???????????? parquet ????????? ????????? ??? ?????? ????????????.

    # write
    df.write.format("parquet").mode("overwrite").save("data/2018-sf-fire-calls.parquet")
    #
    # read
    parquet_df = ss.read.format("parquet").load("data/2018-sf-fire-calls.parquet")

    parquet_df.printSchema()
    parquet_df.show(10)
