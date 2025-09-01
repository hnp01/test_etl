from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import re

# Initialize Spark session
spark = SparkSession.builder.appName("CleanTransformPartD").getOrCreate()

# Read the raw input parquet file
df = spark.read.parquet("outputs/bronze/partd")

# Fix: Rename the correct column name from 'Brnd_Name'
df = df.withColumnRenamed("Brnd_Name", "brand_name")

# Find all columns like 'Tot_Spndng_YYYY'
spending_cols = [c for c in df.columns if re.match(r"Tot_Spndng_\d{4}", c)]

# Build a DataFrame for each year
yearly_dfs = []
for col_name in spending_cols:
    year = int(col_name.split("_")[-1])
    if 2016 <= year <= 2022:
        temp_df = df.select("brand_name", col(col_name).cast("double").alias("total_spending")) \
                    .withColumn("year", lit(year)) \
                    .filter(col("total_spending").isNotNull()) \
                    .filter(col("total_spending") >= 0)
        yearly_dfs.append(temp_df)

# Merge all year-based DataFrames into one
if yearly_dfs:
    final_df = yearly_dfs[0]
    for df_year in yearly_dfs[1:]:
        final_df = final_df.unionByName(df_year)

    # Remove duplicates
    final_df = final_df.dropDuplicates()

    # Write the cleaned data
    final_df.write.mode("overwrite").parquet("outputs/silver/partd")
else:
    print("No matching year columns found.")

spark.stop()