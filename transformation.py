from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("My PySpark code") \
    .getOrCreate()

df = spark.read.options(header='true', inferSchema='true').csv("gs://us-central1-cl-composer-tes-fa29d311-bucket/data/retail_day.csv")
df.printSchema()

df.createOrReplaceTempView("sales")
highestPriceUnitDF = spark.sql("select * from sales where UnitPrice >= 3.0")

highestPriceUnitDF.write.parquet("gs://us-central1-cl-composer-tes-fa29d311-bucket/data/highest_prices.parquet")