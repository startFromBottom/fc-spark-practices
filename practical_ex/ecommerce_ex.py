from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, TimestampType, MapType, \
    BooleanType, FloatType

import pyspark.sql.functions as F


if __name__ == '__main__':
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("ecommerce ex") \
        .getOrCreate()

    # 1. load input data
    input_root_path = "/Users/eomhyeonho/Workspace/spark-practices/practical_ex/input"

    # 1.1 products
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("category_ids", ArrayType(IntegerType()), False),
    ])

    products_df = ss.read \
        .option("inferSchema", False) \
        .json(f"{input_root_path}/products.json") \
        .withColumnRenamed("name", "product_name")

    # 1.2 items
    items_schema = StructType([
        StructField("item_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("seller_id", IntegerType(), False),
        StructField("promotion_ids", ArrayType(IntegerType()), True),
        StructField("original_title", StringType(), False),
        StructField("search_tag", ArrayType(StringType()), True),
        StructField("price", IntegerType(), False),
        StructField("create_timestamp", TimestampType(), False),
        StructField("update_timestamp", TimestampType(), False),
        StructField("attrs", MapType(StringType(), StringType()), False),
        StructField("free_shipping", BooleanType(), False),
    ])

    items_df = ss.read.option("inferSchema", False) \
        .schema(items_schema).json(f"{input_root_path}/items.json") \
        .withColumnRenamed("create_timestamp", "item_create_timestamp") \
        .withColumnRenamed("update_timestamp", "item_update_timestamp") \
        .withColumnRenamed("original_title", "original_item_title")

    # 1.3 categories (product-level)
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False),
    ])
    categories_df = ss.read.option("inferSchema", False) \
        .schema(categories_schema).json(f"{input_root_path}/categories.json")

    # 1.4 review (product-level)
    reviews_schema = StructType([
        StructField("review_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("content", StringType()),
        StructField("score", IntegerType()),
    ])

    reviews_df = ss.read.option("inferSchema", False) \
        .schema(reviews_schema).json(f"{input_root_path}/reviews.json") \
        .withColumnRenamed("score", "review_score") \
        .withColumnRenamed("content", "review_content")

    # 1.5 promotions (item-level)
    promotions_schema = StructType([
        StructField("promotion_id", IntegerType()),
        StructField("name", StringType()),
        StructField("discount_rate", FloatType()),
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
    ])
    promotions_df = ss.read.option("inferSchema", False) \
        .schema(promotions_schema).json(f"{input_root_path}/promotions.json")

    # 1.6 sellers (item-level)
    sellers_schema = StructType([
        StructField("seller_id", IntegerType()),
        StructField("name", StringType())]),
    sellers_df = ss.read.option("inferSchema", False).json(f"{input_root_path}/sellers.json") \
        .withColumnRenamed("name", "seller_name")

    # 2. transformation

    # 2.1 products-level
    def get_categories_dict(categories_df):
        categories = categories_df.rdd.collect()
        res = {}
        for category in categories:
            cat_dict = category.asDict()
            res[cat_dict["category_id"]] = cat_dict
        return res

    categories_dict = get_categories_dict(categories_df)

    def get_categories(category_ids: list):
        return {cid: categories_dict[cid] for cid in category_ids}
    get_categories_udf = udf(get_categories, MapType(IntegerType(), StringType()))

    reviews_score_df = reviews_df.groupby(F.col("product_id"))\
        .agg(F.mean(F.col("review_score")).alias("review_mean_score"),
             F.count(F.col("review_id")).alias("review_count"))

    df1 = products_df.withColumn("categories", get_categories_udf(F.col("category_ids")))\
        .drop(F.col("category_ids")).join(reviews_score_df, on="product_id", how="left")

    # 2.2 items-level
    def get_promotions_dict(promotions_df):
        promotions = promotions_df.rdd.collect()
        res = {}
        for promotion in promotions:
            prom_dict = promotion.asDict()
            res[prom_dict["promotion_id"]] = prom_dict
        return res

    promotions_dict = get_promotions_dict(promotions_df)

    def get_promotions(promotion_ids: list):
        return {pid: promotions_dict[pid] for pid in promotion_ids}
    get_promotions_udf = udf(get_promotions, MapType(IntegerType(), StringType()))

    df2 = items_df.withColumn("promotions", get_promotions_udf(F.col("promotion_ids")))\
        .drop(F.col("promotion_ids")).join(sellers_df, on="seller_id", how="left")

    # 2.3 join product levels and item levels

    final_df = df1.join(df2, on="product_id", how="inner")

    sorted_columns = sorted(final_df.columns)
    final_df = final_df.select(sorted_columns)

    final_df.show()
    final_df.printSchema()

    # write datas

    # 3.1 cassandra
    # docker-desktop에서 cassandra container 실행 시키기!
    # 설정 : # config : https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md#configuration-reference
    final_df.write.format("org.apache.spark.sql.cassandra").mode("append")\
        .option("keyspace", "fc_catalog").option("table", "central_catalog")\
        .option("spark.cassandra.output.consistency.level", "ONE")\
        .save()

    # 3.2 apache iceberg (현재 코드에서 로컬에서는 실행 불가, iceberg docker container 위에서 실행 필요.

    # https://iceberg.apache.org/spark-quickstart/
    # Spark DataFrame의 schema 대로 iceberg table을 생성
    # DDL 문을 이용해 iceberg table을 생성하는 것도 가능.
    # final_df.write.saveAsTable("fc_catalogs.central_catalog")