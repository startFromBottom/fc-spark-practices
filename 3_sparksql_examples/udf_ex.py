from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log dataframe ex") \
        .getOrCreate()


    # 1. udf 함수 정의
    def add_prefix(string):
        return "prefix_" + string

    def cube(n):
        return n ** 3


    # 2. table 준비
    schema = StructType([
        StructField('name', StringType(), False),
        StructField('id', IntegerType(), False),
    ])
    data = [
        ["abc", 1],
        ["ffff", 2],
        ["cccc", 3]
    ]

    # 3. dataframe api
    add_prefix_udf = udf(lambda s: add_prefix(s), StringType())
    cube_udf = udf(lambda n: cube(n), LongType())

    df = ss.createDataFrame(data, schema=schema)
    df.select(
        add_prefix_udf(col("name")).alias("prefix_name"),
        cube_udf(col("id")).alias("cube_id")
    ).show()  # .explain() -> Physical Plan 확인.

    # 4. sql api
    ss.udf.register("add_prefix", add_prefix, StringType())
    ss.udf.register("cube", cube, LongType())

    ss.createDataFrame(data, schema=schema) \
        .createOrReplaceTempView("udf_test")

    ss.sql("""
        SELECT 
            add_prefix(name) AS prefix_name, 
            cube(id) AS cube_id
        FROM udf_test  
    """).show() # .explain() -> Physical Plan 확인.

    # 주의) SparkSQL (SQL, Dataframe 모두 포함) : 하위 표현식의 평가 순서를 보장하지 않음
    # ex) 아래의 예시에서, (name IS NOT NULL), length(add_prefix(name)) > 10
    # 두 조건 중 (name IS NOT NULL)이 항상 먼저 실행된다는 보장 x
    # -> 적절한 null check를 하려면,
    # 1) UDF가 null을 인식하도록 만들고, UDF 내부에서 null 검사 수행
    # 2) SQL에서 IF, CASE WHEN 구문을 이용해 null 검사 수행하고, null이 아닐 때만 UDF 호출.
    ss.sql("""
        SELECT 
            name
        FROM udf_test 
        WHERE name IS NOT NULL AND length(add_prefix(name)) > 10
    """)
