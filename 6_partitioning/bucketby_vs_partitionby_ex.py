import random

from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("partitionby vs bucketby ex") \
        .getOrCreate()

    data = [
        [i, random.randint(20, 40), random.randint(1000, 3000) * 10]
        for i in range(100000)]

    df = ss.createDataFrame(data).toDF("id", "age", "salary")

    df.show()

    # 1. partition by
    # 만들어지는 파일의 개수 : dataframe의 현재 partition 개수 X partitionBy에 지정한 컬럼의 고유한 값의 수.
    # cardinality 가 큰(= 고유한 값의 종류가 많은) 컬럼을 partitionBy에 넣으면
    # 파일이 너무 많이 만들어지는 문제 발생.
    df.repartition(5).write.partitionBy("age") \
        .mode("overwrite").saveAsTable("df_partition_by_age")

    # 2. bucket by
    # 만들어지는 파일의 개수 : dataframe의 현재 partition 개수 X bucketBy에 파라미터로 넣은 정수값.
    # hash function 기반에 따라 파일을 생성하기 때문에,
    # 데이터가 고르게 분배되기 위해서는 cardinality가 큰 컬럼으로 bucketBy하는 것이 좋음.
    # ex)
    # bucketBy(5, "age")
    #     => bucket_number = hash(col("age")) % 5.
    #     각 row는 위의 식으로부터 배정 받은 bucket_number에 해당하는 파일에 저장됨.

    df.repartition(5).write.bucketBy(5, "age") \
        .mode("overwrite").saveAsTable("df_bucket_by_age")

    # 3. partition by + bucket by
    # 만들어지는 파일의 개수 :
    #   (df partition 개수) X (partitionBy에 지정한 컬럼의 고유한 값의 수) X (bucketBy에 파라미터로 넣은 정수 값)
    df.repartition(5).write.bucketBy(5, "salary") \
        .partitionBy("age").mode("overwrite").saveAsTable("df_bucket_by_partiton_by")
