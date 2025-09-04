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
        ORDER_TS::date as ORDER_DATE,
        TRUCK_ID,
        ORDER_TOTAL
    FROM TASTY_BYTES.RAW_POS.ORDER_HEADER
    WHERE ORDER_TS >= DATEADD('month', -36, current_date) -- last 6 months
      AND ORDER_TOTAL IS NOT NULL
"""
)

# -----------------
# Sidebar Filters
# -----------------
trucks = st.sidebar.multiselect(
    "Select Truck IDs:",
    options=sales_df["TRUCK_ID"].unique(),
    default=sales_df["TRUCK_ID"].unique(),
)

filtered_df = sales_df[sales_df["TRUCK_ID"].isin(trucks)]

# -----------------
# KPI Cards
# -----------------
st.title("🚚 Tasty Bytes Sales Dashboard (by Truck)")

col1, col2, col3 = st.columns(3)
col1.metric("Total Sales", f"${filtered_df['ORDER_TOTAL'].sum():,.0f}")
col2.metric("Avg Order", f"${filtered_df['ORDER_TOTAL'].mean():,.0f}")
col3.metric("Orders", f"{filtered_df.shape[0]:,}")

# -----------------
# Charts
# -----------------
st.subheader("Sales Over Time")
st.line_chart(filtered_df.groupby("ORDER_DATE")["ORDER_TOTAL"].sum())

st.subheader("Top Trucks by Sales")
truck_sales = (
    filtered_df.groupby("TRUCK_ID")["ORDER_TOTAL"].sum().sort_values(ascending=False)
)
st.bar_chart(truck_sales)
