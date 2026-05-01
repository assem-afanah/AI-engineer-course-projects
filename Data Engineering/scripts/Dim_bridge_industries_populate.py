import pandas as pd
import pyodbc
import config

# -----------------------------------
# 1. Connect to SQL Server
# -----------------------------------
conn = pyodbc.connect(
    "DRIVER={" + config.driver + "};"
    "SERVER=" + config.server + ";"
    "DATABASE=" + config.database + ";"
    "Trusted_Connection=yes;"
)

# -----------------------------------
# 2. Extract distinct industry
# -----------------------------------
query = """
SELECT job_id
      , industry_id , job_id
      , industry_id
  FROM buisnessDB.job_industries
  join dwh.fact_posting on fact_posting.posting_key = job_industries.job_id
  join dwh.dim_industry on dim_industry.industry_key = job_industries.industry_id
  where dim_industry.industry_key is not null and fact_posting.posting_key is not null
;
"""

df_industries = pd.read_sql(query, conn)
# -----------------------------------
# 3. Insert into DWH.bridge_posting_industry
# -----------------------------------
cursor = conn.cursor()

batch_size = 1000
for i in range(0, len(df_industries), batch_size):
    rows_to_insert = df_industries.iloc[i:i+batch_size]
    cursor.executemany("""
        IF NOT EXISTS (
            SELECT 1
            FROM DWH.bridge_posting_industry
            WHERE posting_key = ?
              AND industry_key   = ?
        )
        INSERT INTO DWH.bridge_posting_industry (posting_key, industry_key)
        VALUES (?, ?)
    """, 
    rows_to_insert.values.tolist()
    )
    conn.commit()

cursor.close()
conn.close()

print("✅ bridge_posting_industry populated successfully!")