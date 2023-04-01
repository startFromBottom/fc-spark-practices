from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[4]") \
        .appName("spark_dpp_ex") \
        .getOrCreate()

    # spark에서 자동으로 broadcasting.
    # (default spark.sql.autoBroadcastJoinThreshold=10MB)
    users_ddl = """
    CREATE TABLE Users
    USING parquet
    AS
      SELECT id AS uid,
      CAST(rand() * 10 AS INT) AS grade
    FROM RANGE(100);
    """
    ss.sql(users_ddl)

    orders_ddl = """
    CREATE TABLE Orders
    USING parquet
    PARTITIONED BY (uid)
    AS
      SELECT
        CAST((rand()*100) AS INT) AS uid,
        CAST((rand()*100) AS INT) AS quantity
    FROM RANGE (1000000);
    """
    ss.sql(orders_ddl)

    # 1. partition pruning, predicate pushdown
    # 이 sql문은 WHERE 조건에 명시된 3개의 uid에 해당하는 파일만 읽어옴.
    # = 조건, range query에서 모두 잘 동작.
    pp_dml = """
    SELECT uid, quantity * 2 AS double_quantity
    FROM Orders
    WHERE uid >= 5 AND uid <= 7
    """
    ss.sql(pp_dml).count()

    # 2. dynamic partition pruning
    # WHERE 문에서 Users 테이블의 컬럼을 사용 했지만,
    # Users가 아닌, Orders 테이블의 데이터를 불러올 때
    # 일부 파티션의 데이터만 읽을 수 있음!
    dpp_dml = """
    SELECT u.uid, o.quantity
    FROM Users AS u
      JOIN Orders AS o
      ON u.uid = o.uid
    WHERE u.grade = 7
    """

    ss.sql(dpp_dml).count()

    while True:
        pass
