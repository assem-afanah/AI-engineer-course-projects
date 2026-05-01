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
# 2. Extract distinct company
# -----------------------------------
query = """
SELECT company_id ,  name
FROM buisnessDB.companies
WHERE name IS NOT NULL
;
"""

df_industries = pd.read_sql(query, conn)
# -----------------------------------
# 3. Insert into DWH.dim_company
# -----------------------------------
cursor = conn.cursor()

for _, row in df_industries.iterrows():
    print(row)
    cursor.execute("""
                   
        SET IDENTITY_INSERT DWH.dim_company ON;
                   
        INSERT INTO DWH.dim_company (company_key, name)
        VALUES (?, ?)
    """,
    row["company_id"], row["name"]
    )

conn.commit()
cursor.close()
conn.close()

print("✅ dim_company populated successfully!")
