from pyspark.sql import SparkSession

if __name__ == "__main__":

    ss: SparkSession = SparkSession.builder \
        .master("local[4]") \
        .config("spark.sql.join.preferSortMergeJoin", True) \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .appName("bucketby_join ex") \
        .getOrCreate()

    # 1. without bucketing.
    data1 = [
        [i, i % 2] for i in range(1000000)
    ]
    df1 = ss.createDataFrame(data1).toDF("id", "value")

    data2 = [
        [i, i % 3] for i in range(500000)
    ]
    df2 = ss.createDataFrame(data2).toDF("id", "value")

    joined_df = df1.join(df2, on="id", how="inner")
    joined_df.count()

    # 2. with bucketing
    df1.write.bucketBy(5, "id") \
        .mode("overwrite").saveAsTable("df1_bucket")

    df2.write.bucketBy(6, "id") \
        .mode("overwrite").saveAsTable("df2_bucket")

    bucket_df1 = ss.read.table("df1_bucket")
    bucket_df2 = ss.read.table("df2_bucket")

    bucket_joined_df = bucket_df1.join(bucket_df2, on="id",
                                       how="inner")
    bucket_joined_df.count()

    while True:
        pass
