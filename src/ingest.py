from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IngestPartD").getOrCreate()
raw = spark.read.option("header", True).csv("data/raw/*.csv")
raw.write.mode("overwrite").parquet("outputs/bronze/partd")
spark.stop()
