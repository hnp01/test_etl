################################

# Compute standard deviation or variance of spending over years.
# Drugs with high volatility may be influenced by generics, regulatory changes, or new competitors.

# Spending vs. Claims Efficiency
# Identify drugs that cost disproportionately more per claim.
# High-spend/low-claim drugs could signal high-cost specialty drugs or inefficiencies.
# df['spend_per_claim'] = df['total_spending'] / df['total_claims']


# Year-over-Year Growth
# Spot fast-growing drugs in terms of spending or usage.


######################

import glob
import pandas as pd
import streamlit as st
import plotly.express as px

# Load and concatenate CSV files
csv_files = glob.glob("outputs/gold/agg_ranked_drug_spending/*.csv")
df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

# Normalize column names
df.columns = [col.strip().lower() for col in df.columns]

# Ensure correct types
df['rank'] = pd.to_numeric(df['rank'], errors='coerce')
df['year'] = pd.to_numeric(df['year'], errors='coerce')
df['total_spending'] = pd.to_numeric(df['total_spending'], errors='coerce')

# Sidebar control
st.sidebar.title("Filters")
top_n = st.sidebar.slider("Select Top N Drugs per Year", min_value=5, max_value=50, value=10)

# Filter to top N ranked drugs per year
top_df = df.sort_values(by=['year', 'rank']).groupby('year').head(top_n)

# Main content
st.title("Drug Spending Rank Changes Over Years")

brands = top_df['brnd_name'].unique()
selected_brands = st.multiselect("Select brands to display", brands, default=brands[:10])

# Apply brand selection
filtered_df = top_df[top_df['brnd_name'].isin(selected_brands)]

# --- Tab Layout ---
tab1, tab2, tab3 = st.tabs(["📊 Spending Animation", "📈 Relative Volatility", "📉 CAGR Analysis"])

# --- Tab 1: Animated Bar Chart ---
with tab1:
    st.header("Top Drug Brands by Total Spending Over Time")

    fig = px.bar(
        filtered_df,
        x='total_spending',
        y='brnd_name',
        color='brnd_name',
        animation_frame='year',
        orientation='h',
        range_x=[0, filtered_df['total_spending'].max() * 1.1],
        title="Top Drug Brands by Total Spending (Animated by Year)"
    )

    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title='Total Spending',
        yaxis_title='Brand Name',
        legend_title='Brand'
    )

    st.plotly_chart(fig, use_container_width=True)
    
with tab2:
    st.header("Relative Volatility (CV of Spending)")

    spd_volatility = df.groupby('brnd_name')['total_spending'].agg(['mean', 'std'])
    spd_volatility['RSD (%)'] = (spd_volatility['std'] / spd_volatility['mean']) * 100
    
    spd_volatility_filtered = spd_volatility[spd_volatility.index.isin(selected_brands)].round(2).sort_values('RSD (%)', ascending=False)

    # Table
    st.dataframe(
        spd_volatility_filtered[['mean', 'std', 'RSD (%)']].rename(columns={
            'mean': 'Avg Spending',
            'std': 'Spending Std Dev'
        })
    )

    # Bar Chart
    st.subheader("Bar Chart of Relative Volatility")
    fig_rsd = px.bar(
        spd_volatility_filtered.reset_index(),
        x='brnd_name',
        y='RSD (%)',
        title="Relative Standard Deviation of Spending (Volatility)",
        labels={'RSD (%)': 'RSD (%)', 'brnd_name': 'Brand Name'},
        color='brnd_name'
    )
    fig_rsd.update_layout(xaxis_title='Brand Name', yaxis_title='RSD (%)')
    st.plotly_chart(fig_rsd, use_container_width=True)
    
    
with tab3:
    st.header("CAGR (Compound Annual Growth Rate) of Spending")

    def compute_cagr_row(row):
        start = row['min']
        end = row['max']
        periods = row['count'] - 1
        if periods <= 0 or start <= 0:
            return float('nan')
        return (end / start) ** (1 / periods) - 1

    grouped = df.groupby('brnd_name')['total_spending'].agg(['min', 'max', 'count'])
    grouped['CAGR'] = grouped.apply(compute_cagr_row, axis=1)

    grouped_filtered = grouped.loc[selected_brands]
    grouped_filtered['CAGR'] = grouped_filtered['CAGR'].apply(lambda x: round(x * 100, 2)) # %
    grouped_filtered= grouped_filtered.sort_values('CAGR', ascending=False)  

    # Table
    st.dataframe(grouped_filtered[['min', 'max', 'count', 'CAGR']].rename(columns={
        'min': 'Min Spending',
        'max': 'Max Spending',
        'count': 'Years',
        'CAGR': 'CAGR (%)'
    }))

    # Bar Chart
    st.subheader("Bar Chart of CAGR (%)")
    fig_cagr = px.bar(
        grouped_filtered.reset_index(),
        x='brnd_name',
        y='CAGR',
        title="Compound Annual Growth Rate (%) by Brand",
        labels={'CAGR': 'CAGR (%)', 'brnd_name': 'Brand Name'},
        color='brnd_name'
    )
    st.plotly_chart(fig_cagr, use_container_width=True)