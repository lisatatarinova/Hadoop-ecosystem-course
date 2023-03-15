from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark DF practice').master('yarn').getOrCreate()

from pyspark.sql.types import *
from pyspark.sql.functions import col, lower, regexp_replace, concat_ws, count
import pyspark.sql.functions as f
from pyspark.sql import Window


schema = StructType(fields=[
    StructField("article_id", IntegerType()),
    StructField("article", StringType())
])

df = spark.read.format("csv") \
    .schema(schema) \
    .option("sep", "\t") \
    .load("/data/wiki/en_articles_part")

schema2 = StructType(fields=[
    StructField("article_id", IntegerType()),
    StructField("word", StringType())
])

df2 = spark.createDataFrame([], schema2)
df2 = df.select(
    "article_id",
    f.posexplode(f.split("article", " ")).alias("pos", "val"))

df2.repartition("article_id")

df3 = spark.createDataFrame([], schema2)
df3 = df2.select(["article_id", "pos", lower(col('val')).alias("val")]).orderBy("article_id","pos")

df4 = spark.createDataFrame([], schema2)
df4 = df3.withColumn("val", regexp_replace('val', '^\W+|\W+$', ''))

df_ready_to_analysis = spark.createDataFrame([], schema2)
df_ready_to_analysis = df4.select("article_id", "val").where(f.length("val")>=1)

schema3 = StructType(fields=[
    StructField("article_id", IntegerType()),
    StructField("word", StringType()),
    StructField("next_word", StringType())
])

new_df = spark.createDataFrame([], schema3)

new_df = df_ready_to_analysis.select("article_id", "val", f.lead(f.col("val"),default=0) \
                                     .over(Window.orderBy("article_id")).alias("next_word"))


result_df = spark.createDataFrame([], schema2)
#result_df = new_df.select(["article_id", "val"]).where(col("val")=="narodnaya")

result_df = new_df.select(["article_id", concat_ws('_', "val", "next_word").alias("bigramm")]).where(col("val")=="narodnaya")

schema4 = StructType(fields=[
    StructField("article_id", IntegerType()),
    StructField("word", StringType()),
    StructField("count", IntegerType())
])
main_result_df = spark.createDataFrame([], schema4)

main_result_df = result_df.groupBy("bigramm").count()

main_result_df2 = spark.createDataFrame([], schema4)

main_result_df2 = main_result_df.sort("bigramm")

for row in main_result_df2.collect(): print(row['bigramm'].encode("US-ASCII") + ' ' + str(row['count']))
