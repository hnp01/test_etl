import pandas as pd
import plotly.express as px
import glob

# Locate and read the CSV exported by Spark
csv_files = glob.glob("outputs/gold/agg_ranked_drug_spending/part-00000*.csv")
if not csv_files:
    raise FileNotFoundError("No CSV file found in outputs/gold/agg_ranked_drug_spending/")

df = pd.read_csv(csv_files[0])

# Clean column names if necessary
df.columns = [col.strip().lower() for col in df.columns]

# Check for expected columns
required_cols = {"brnd_name", "year", "total_spending", "rank"}
if not required_cols.issubset(set(df.columns)):
    raise ValueError(f"Missing expected columns in CSV. Found: {df.columns}")

# Optional: Keep only top N brands based on appearance or rank
top_brands = df[df["rank"] <= 5]["brnd_name"].unique()
df_filtered = df[df["brnd_name"].isin(top_brands)]

# Line chart showing ranking over years (lower rank = better)
fig = px.line(
    df_filtered,
    x="year",
    y="rank",
    color="brnd_name",
    markers=True,
    text="total_spending",
    title="Top Drug Brand Rankings Over Time",
    labels={"rank": "Rank (1 = Highest Spending)", "brnd_name": "Brand Name"},
)

fig.update_traces(texttemplate="%{text:.2s}", mode="lines+markers")
fig.update_yaxes(autorange="reversed")  # Lower rank is better
fig.update_layout(
    hovermode="x unified",
    template="plotly_white",
    legend_title="Brand",
    height=600,
    margin=dict(t=50, b=50, l=40, r=10)
)

# fig.show()
fig.write_html("top_brands_rank_plot.html", auto_open=True)
