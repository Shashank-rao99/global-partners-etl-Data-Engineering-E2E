import streamlit as st
from pyathena import connect
import pandas as pd

# Athena connection (same config as before)
conn = connect(
    s3_staging_dir='s3://global-partners-project/athena-results/',
    region_name='us-east-1',
    schema_name='globalpartners_db'
)

st.title("Global Partners Business Insights Dashboard")

# Example: Customer CLV Segmentation
query_clv = """
WITH item_revenue AS (
  SELECT 
      oi.user_id,
      CAST(oi.item_price AS DOUBLE) * CAST(oi.item_quantity AS INTEGER) AS base_item_total,
      COALESCE(SUM(CAST(oio.option_price AS DOUBLE) * CAST(oio.option_quantity AS INTEGER)), 0) AS options_total
  FROM order_items oi
  LEFT JOIN order_item_options oio
    ON oi.order_id = oio.order_id AND oi.lineitem_id = oio.lineitem_id
  GROUP BY oi.user_id, oi.order_id, oi.lineitem_id, oi.item_price, oi.item_quantity
),
customer_clv AS (
  SELECT
    user_id,
    SUM(base_item_total + options_total) AS total_lifetime_value
  FROM item_revenue
  GROUP BY user_id
)
SELECT
  user_id,
  total_lifetime_value,
  CASE
    WHEN ntile(5) OVER (ORDER BY total_lifetime_value DESC) = 1 THEN 'High CLV'
    WHEN ntile(5) OVER (ORDER BY total_lifetime_value DESC) IN (2, 3, 4) THEN 'Medium CLV'
    ELSE 'Low CLV'
  END AS clv_tag
FROM customer_clv
ORDER BY total_lifetime_value DESC
"""

df_clv = pd.read_sql(query_clv, conn)
st.subheader("Customer Lifetime Value (CLV) Segmentation")
st.dataframe(df_clv)

query_rfm = """
WITH purchases AS (
  SELECT
    user_id,
    CAST(from_iso8601_timestamp(creation_time_utc) AS DATE) AS order_date,
    CAST(item_price AS DOUBLE) * CAST(item_quantity AS INTEGER) AS order_value
  FROM order_items
)
, summary AS (
  SELECT
    user_id,
    MAX(order_date) AS last_order_date,
    COUNT(*) AS frequency,
    SUM(order_value) AS monetary
  FROM purchases
  GROUP BY user_id
)
SELECT
  user_id,
  CAST(date_diff('day', last_order_date, current_date) AS INTEGER) AS recency,
  frequency,
  monetary
FROM summary
ORDER BY monetary DESC
"""

df_rfm = pd.read_sql(query_rfm, conn)
st.subheader("Customer Segmentation (RFM Analysis)")
st.dataframe(df_rfm)

query_churn = """
WITH user_orders AS (
  SELECT
    user_id,
    CAST(from_iso8601_timestamp(creation_time_utc) AS DATE) AS order_date,
    CAST(item_price AS DOUBLE) * CAST(item_quantity AS INTEGER) AS order_value
  FROM order_items
)
, churn_calc AS (
  SELECT
    user_id,
    MAX(order_date) AS last_order_date,
    MIN(order_date) AS first_order_date,
    COUNT(*) AS total_orders,
    SUM(order_value) AS total_spend,
    AVG(order_value) AS avg_order_value
  FROM user_orders
  GROUP BY user_id
)
SELECT
  user_id,
  last_order_date,
  first_order_date,
  total_orders,
  total_spend,
  avg_order_value,
  date_diff('day', last_order_date, current_date) AS days_since_last_order,
  CASE
    WHEN date_diff('day', last_order_date, current_date) > 45 THEN 'At Risk'
    ELSE 'Active'
  END AS churn_status
FROM churn_calc
ORDER BY days_since_last_order DESC
"""

df_churn = pd.read_sql(query_churn, conn)
st.subheader("Churn Indicators")
st.dataframe(df_churn)

query_loyalty = """
WITH orders_loyalty AS (
  SELECT
    user_id,
    is_loyalty,
    CAST(item_price AS DOUBLE) * CAST(item_quantity AS INTEGER) AS order_value
  FROM order_items
)
, summary AS (
  SELECT
    is_loyalty,
    COUNT(DISTINCT user_id) AS unique_customers,
    COUNT(*) AS total_orders,
    AVG(order_value) AS avg_order_value,
    SUM(order_value) AS total_spend
  FROM orders_loyalty
  GROUP BY is_loyalty
)
SELECT
  is_loyalty,
  unique_customers,
  total_orders,
  avg_order_value,
  total_spend
FROM summary
"""

df_loyalty = pd.read_sql(query_loyalty, conn)
st.subheader("Loyalty Program Impact")
st.dataframe(df_loyalty)

query_sales_trends = """
WITH orders AS (
  SELECT
    date(from_iso8601_timestamp(creation_time_utc)) AS order_date,
    EXTRACT(month FROM from_iso8601_timestamp(creation_time_utc)) AS month,
    EXTRACT(year FROM from_iso8601_timestamp(creation_time_utc)) AS year,
    restaurant_id,
    item_category,
    order_id,
    item_price,
    item_quantity
  FROM order_items
)
SELECT
  order_date,
  month,
  year,
  restaurant_id,
  item_category,
  SUM(item_price * item_quantity) AS total_revenue,
  COUNT(DISTINCT order_id) AS total_orders
FROM orders
GROUP BY order_date, month, year, restaurant_id, item_category
ORDER BY year, month, order_date
"""

df_sales_trends = pd.read_sql(query_sales_trends, conn)
st.subheader("Sales Trends and Seasonality")
st.dataframe(df_sales_trends)

query_top_performing_locations = """
WITH orders AS (
  SELECT
    restaurant_id,
    order_id,
    date(from_iso8601_timestamp(creation_time_utc)) AS order_date,
    CAST(item_price AS DOUBLE) * CAST(item_quantity AS INTEGER) AS order_value
  FROM order_items
)
SELECT
  restaurant_id,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(order_value) AS total_revenue,
  AVG(order_value) AS avg_order_value,
  COUNT(DISTINCT order_date) AS days_open,
  (SUM(order_value) / COUNT(DISTINCT order_date)) AS avg_daily_revenue
FROM orders
GROUP BY restaurant_id
ORDER BY total_revenue DESC
"""

df_top_performing_locations = pd.read_sql(query_top_performing_locations, conn)
st.subheader("Top-Performing Locations")
st.dataframe(df_top_performing_locations)

query_pricing  = """
WITH base_orders AS (
  SELECT
    oi.order_id,
    oi.user_id,
    oi.restaurant_id,
    CAST(oi.item_price AS DOUBLE) * CAST(oi.item_quantity AS INTEGER) AS item_total
  FROM order_items oi
),
option_discounts AS (
  SELECT
    oio.order_id,
    oio.lineitem_id,
    SUM(CASE WHEN CAST(option_price AS DOUBLE) < 0
        THEN CAST(option_price AS DOUBLE) * CAST(option_quantity AS INTEGER)
        ELSE 0 END) AS total_discount
  FROM order_item_options oio
  GROUP BY oio.order_id, oio.lineitem_id
),
order_summary AS (
  SELECT
    bo.order_id,
    bo.user_id,
    bo.restaurant_id,
    SUM(bo.item_total) AS order_gross,
    COALESCE(SUM(od.total_discount), 0) AS order_discount
  FROM base_orders bo
  LEFT JOIN option_discounts od
    ON bo.order_id = od.order_id
  GROUP BY bo.order_id, bo.user_id, bo.restaurant_id
)
SELECT
  CASE 
    WHEN order_discount < 0 THEN 'Discounted' 
    ELSE 'Full Price' 
  END AS order_type,
  COUNT(*) AS num_orders,
  SUM(order_gross + order_discount) AS net_revenue,
  AVG(order_gross + order_discount) AS avg_net_order_value
FROM order_summary
GROUP BY
  CASE 
    WHEN order_discount < 0 THEN 'Discounted' 
    ELSE 'Full Price' 
  END
ORDER BY net_revenue DESC
"""

df_pricing = pd.read_sql(query_pricing, conn)
st.subheader("Pricing & Discount Effectiveness")
st.dataframe(df_pricing)