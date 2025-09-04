import pandas as pd
import snowflake.connector
import streamlit as st

# -----------------
# Sidebar Config
# -----------------
st.set_page_config(page_title="Sales Dashboard", layout="wide")
st.sidebar.header("Filters")


# -----------------
# Snowflake Connection
# -----------------
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
    )


conn = init_connection()


# -----------------
# Query Helper
# -----------------
@st.cache_data(ttl=600)
def run_query(query):
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    return df


# -----------------
# Load Data
# -----------------
sales_df = run_query(
    """
    SELECT 
        ORDER_DATE::date as ORDER_DATE,
        REGION,
        PRODUCT,
        SALES_AMOUNT
    FROM ORDERS
    WHERE ORDER_DATE >= DATEADD('month', -6, current_date) -- last 6 months
"""
)

# -----------------
# Sidebar Filters
# -----------------
regions = st.sidebar.multiselect(
    "Select Regions:",
    options=sales_df["REGION"].unique(),
    default=sales_df["REGION"].unique(),
)

products = st.sidebar.multiselect(
    "Select Products:",
    options=sales_df["PRODUCT"].unique(),
    default=sales_df["PRODUCT"].unique(),
)

filtered_df = sales_df[
    (sales_df["REGION"].isin(regions)) & (sales_df["PRODUCT"].isin(products))
]

# -----------------
# KPI Cards
# -----------------
st.title("📊 Sales Dashboard (Snowflake Powered)")

col1, col2, col3 = st.columns(3)
col1.metric("Total Sales", f"${filtered_df['SALES_AMOUNT'].sum():,.0f}")
col2.metric("Avg Order", f"${filtered_df['SALES_AMOUNT'].mean():,.0f}")
col3.metric("Orders", f"{filtered_df.shape[0]:,}")

# -----------------
# Charts
# -----------------
st.subheader("Sales Over Time")
st.line_chart(filtered_df.groupby("ORDER_DATE")["SALES_AMOUNT"].sum())

st.subheader("Top Products")
top_products = (
    filtered_df.groupby("PRODUCT")["SALES_AMOUNT"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)
st.bar_chart(top_products)

st.subheader("Sales by Region")
region_sales = filtered_df.groupby("REGION")["SALES_AMOUNT"].sum()
st.bar_chart(region_sales)
