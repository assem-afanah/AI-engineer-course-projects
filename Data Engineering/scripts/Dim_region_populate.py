import pandas as pd
import pyodbc
import config

def is_valid_value(value: Any) -> bool:
    """Return True if value is valid, False otherwise"""
    if value is None:
        return False
    value_str = str(value).strip()  # Convert to string
    if value_str == "":
        return False
    if "," in value_str:
        return False
    if value_str.isnumeric():
        return False
    if value_str.lower() == "nan":
        return False
    return True



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
# 2. Extract distinct locations
# -----------------------------------
query = """
SELECT DISTINCT
    country,
    state,
    city
FROM buisnessDB.companies
WHERE city IS NOT NULL and city != '0' and city NOT LIKE '%,%'
;
"""

df_locations = pd.read_sql(query, conn)
# -----------------------------------
# 3. Insert into DWH.dim_location
# -----------------------------------
cursor = conn.cursor()

for _, row in df_locations.iterrows():

    print(row["country"], row["state"], row["city"])
    if is_valid_value(row["country"]) is False:
        row["country"] = ''

    if  is_valid_value(row["state"]) is False:
        row["state"] = ''
        

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1
            FROM DWH.dim_region
            WHERE country = ?
              AND state   = ?
              AND city    = ?
        )
        INSERT INTO DWH.dim_region (country, state, city)
        VALUES (?, ?, ?)
    """,
    row["country"], row["state"], row["city"],
    row["country"], row["state"], row["city"]
    )

conn.commit()
cursor.close()
conn.close()

print("✅ dim_location populated successfully!")
