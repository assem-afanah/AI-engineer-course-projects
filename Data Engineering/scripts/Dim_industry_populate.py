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
SELECT industry_id , industry_name
FROM buisnessDB.industries
WHERE industry_name IS NOT NULL
;
"""

df_industries = pd.read_sql(query, conn)
# -----------------------------------
# 3. Insert into DWH.dim_industry
# -----------------------------------
cursor = conn.cursor()

for _, row in df_industries.iterrows():
    print(row)
    cursor.execute("""
        SET IDENTITY_INSERT DWH.dim_industry ON;

        INSERT INTO DWH.dim_industry (industry_key, industry_name)
        VALUES (?, ?)
    """,
    row["industry_id"], row["industry_name"]
    )

conn.commit()
cursor.close()
conn.close()

print("✅ dim_industry populated successfully!")
