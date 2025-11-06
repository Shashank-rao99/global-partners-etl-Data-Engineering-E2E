from pyathena import connect
import pandas as pd

conn = connect(
    s3_staging_dir='s3://global-partners-project/athena-results/',
    region_name='us-east-1',
    schema_name='globalpartners_db'
)

# Try a simple query
df = pd.read_sql("SELECT * FROM order_items LIMIT 5;", conn)
print(df)