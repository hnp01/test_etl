from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("AggregatePartD").getOrCreate()
df = spark.read.parquet("outputs/silver/partd")

window = Window.partitionBy("year").orderBy(col("total_spending").desc())
top = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)
top.select("year", "brand_name", "total_spending").show()

yearly = df.groupBy("year").agg(sum_("total_spending").alias("total_spend"))
yearly.write.mode("overwrite").parquet("outputs/gold/yearly_spend")
spark.stop()
