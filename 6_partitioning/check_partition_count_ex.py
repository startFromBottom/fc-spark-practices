from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("check partition count ex") \
        .getOrCreate()

    # 1. rdd
    sc: SparkContext = ss.sparkContext

    num_partitions = 10

    data = sc.parallelize(range(30), 10)
    print(f"number of partitions => {data.getNumPartitions()}")
    print(f"partition structure => {data.glom().collect()}")

    data = data.repartition(5)
    print("=== after repartition ===")
    print(f"number of partitions => {data.getNumPartitions()}")
    print(f"partition structure => {data.glom().collect()}")

    # 2. dataframe
    df = ss.createDataFrame([[i] for i in range(30)]).toDF("id")
    df = df.repartition(10)

    print(f"the number of dataframe partition => {df.rdd.getNumPartitions()}")
