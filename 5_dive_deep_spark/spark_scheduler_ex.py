from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[4]") \
        .appName("spark_scheduler_ex") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    df1 = ss.read.text("data/l1.txt").toDF("id")
    df2 = ss.read.text("data/l2.txt").toDF("id")
    df3 = ss.read.text("data/l3.txt").toDF("id")
    df4 = ss.read.text("data/l4.txt").toDF("id")

    # action 1
    print(df1.join(df2, "id", "inner").count())

    # action 2
    print(df3.join(df4, "id", "inner").count())

    # action 3
    print(df4.join(df2, "id", "full_outer").count())

    # action 1,2,3 -> independent!

    # while True:
    #     pass
