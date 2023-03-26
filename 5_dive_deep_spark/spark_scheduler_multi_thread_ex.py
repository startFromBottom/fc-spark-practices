import threading
from pyspark.sql import SparkSession


def do_spark_job(f1, f2):
    df1 = ss.read.text(f1).toDF("id")
    df2 = ss.read.text(f2).toDF("id")
    outputs.append(df1.join(df2, "id", "inner").count())


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[3]") \
        .appName("spark_scheduler_multi_thread_ex") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.scheduler.mode", "FAIR") \
        .getOrCreate()

    file_prefix = "data/l"

    jobs = []
    outputs = []

    for i in range(3):
        file1 = f"{file_prefix}{i + 1}.txt"
        file2 = f"{file_prefix}{i + 2}.txt"
        thread = threading.Thread(target=do_spark_job, args=(file1, file2))
        jobs.append(thread)

    for j in jobs:
        j.start()

    for j in jobs:
        j.join()

    # while True:
    #     pass
