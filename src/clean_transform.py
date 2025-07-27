from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CleanTransformPartD").getOrCreate()
df = spark.read.parquet("outputs/bronze/partd")

df = df.withColumnRenamed("Brand_Name", "brand_name") \
       .withColumn("year", col("Year").cast("int")) \
       .withColumn("total_spending", col("Total_Spending").cast("double")) \
       .filter(col("year").between(2016, 2022)) \
       .filter(col("total_spending") >= 0) \
       .dropDuplicates()

df.write.mode("overwrite").parquet("outputs/silver/partd")
spark.stop()
