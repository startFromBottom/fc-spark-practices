import random

from pyspark.rdd import portable_hash
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def print_partitions_info(df: DataFrame):
    num_partitions = df.rdd.getNumPartitions()
    print("Total partitions: {}\n".format(num_partitions))
    print("Partitioner: {}\n".format(df.rdd.partitioner))
    df.explain()
    print("\n")
    parts = df.rdd.glom().collect()

    for i, part in enumerate(parts):
        print("\nPartition {}:".format(i))
        for j, r in enumerate(part):
            print("Row {}:{}".format(j, r))


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("sort_merge_join_ex") \
        .getOrCreate()

    subjects = ["Math", "English", "Science"]

    data = [
        [i, subjects[i % 3], random.randint(1, 100)] for i in range(100)
    ]

    df = ss.createDataFrame(data).toDF("id", "subject", "score")
    score_df = df.repartition(3, "subject")

    # 3개의 partition(Partition 0, 1, 2) 중에
    # Partiton 1, 2에만 데이터가 들어가 있음.
    print_partitions_info(score_df)

    # 각 row 별 hash 값 출력
    # English, Science subject 의 hash 값을 3으로 나눈 나머지가 같기 때문에,
    # English, Science 는 같은 Partition으로 들어가게 됨.
    # -> skewed partition의 원인이 될 수 있음!
    udf_portable_hash = F.udf(lambda s: portable_hash(s))
    score_df = score_df.withColumn("Hash#",
                                   udf_portable_hash(score_df.subject))
    score_df = score_df.withColumn("Partition#", score_df["Hash#"] % 3)
    score_df.orderBy("id").show()
    score_df.explain()


    # custom partitioning.
    def subject_partitioning(k):
        return subjects.index(k)


    udf_subject_hash = F.udf(lambda s: subject_partitioning(s))

    num_partitions = 3
    # if scala or java
    # custom_df = df.partitionBy(num_partitions, subjectPartitioner)

    custom_df = df.withColumn("Hash#", udf_subject_hash(df['subject']))
    custom_df = custom_df.withColumn("Partition#", custom_df["Hash#"]
                                     % num_partitions)
    custom_df.orderBy("id").show()

    print_partitions_info(df.repartition(3, "Partition#"))
