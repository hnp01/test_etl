
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as Fsum, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
import re

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("AggregateAndRankDrugSpending").getOrCreate()

# Step 2: Read bronze-level data
df = spark.read.parquet("outputs/bronze/partd")

# Step 3: Identify relevant columns
spending_cols = [c for c in df.columns if re.match(r"Tot_Spndng_\d{4}", c)]
claims_cols = [c for c in df.columns if re.match(r"Tot_Clms_\d{4}", c)]

# Step 4: Create aggregated data for each year
agg_dfs = []

for spend_col in spending_cols:
    year = int(spend_col[-4:])
    if 2016 <= year <= 2022:
        claims_col = f"Tot_Clms_{year}"
        if claims_col in claims_cols:
            agg_df = df.groupBy("Brnd_Name").agg(
                Fsum(col(spend_col).cast("double")).alias("total_spending"),
                Fsum(col(claims_col).cast("double")).alias("total_claims")
            ).withColumn("year", lit(year))

            # Apply ranking
            window_spec = Window.orderBy(col("total_spending").desc())
            ranked_df = agg_df.withColumn("rank", rank().over(window_spec))

            agg_dfs.append(ranked_df)

# Step 5: Union all years
if agg_dfs:
    final_df = agg_dfs[0]
    for df_year in agg_dfs[1:]:
        final_df = final_df.unionByName(df_year)

    # Step 6: Write to CSV
    final_df.orderBy("year", "total_spending", "rank") \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("outputs/gold/agg_ranked_drug_spending")
else:
    print("No matching year columns found for aggregation.")

spark.stop()