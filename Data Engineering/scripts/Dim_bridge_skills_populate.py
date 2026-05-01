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
# 2. Extract data
# -----------------------------------
query = """
SELECT job_id
      , skill_abr ,  job_id
      , skill_abr
  FROM buisnessDB.job_skills
  join dwh.fact_posting on fact_posting.posting_key = job_skills.job_id
  where fact_posting.posting_key is not null
;
"""

df_skills = pd.read_sql(query, conn)
# -----------------------------------
# 3. Insert into DWH.bridge_posting_skill
# -----------------------------------
cursor = conn.cursor()

batch_size = 1000
for i in range(0, len(df_skills), batch_size):
    rows_to_insert = df_skills.iloc[i:i+batch_size]
    cursor.executemany("""
        IF NOT EXISTS (
            SELECT 1
            FROM DWH.bridge_posting_skill
            WHERE posting_key = ?
              AND skill_key   = ?
        )
        INSERT INTO DWH.bridge_posting_skill (posting_key, skill_key)
        VALUES (?, ?)
    """, 
    rows_to_insert.values.tolist()
    )
    conn.commit()

cursor.close()
conn.close()

print("✅ bridge_posting_skill populated successfully!")