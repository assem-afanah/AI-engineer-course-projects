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
# 2. Extract distinct skill
# -----------------------------------
query = """
SELECT skill_abr , skill_name
FROM buisnessDB.skills
WHERE skill_name IS NOT NULL
;
"""

df_industries = pd.read_sql(query, conn)
# -----------------------------------
# 3. Insert into DWH.dim_skill
# -----------------------------------
cursor = conn.cursor()

for _, row in df_industries.iterrows():
    cursor.execute("""
        INSERT INTO DWH.dim_skill (skill_key, skill_name)
        VALUES (?, ?)
    """,
    row["skill_abr"], row["skill_name"]
    )

conn.commit()
cursor.close()
conn.close()

print("✅ dim_skill populated successfully!")
