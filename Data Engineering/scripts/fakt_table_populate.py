import pandas as pd
import pyodbc
from datetime import datetime
import config

# -----------------------------------
# 1. Connect to SQL Server
# -----------------------------------
connconfig = "DRIVER={" + config.driver + "}; SERVER=" + config.server + "; DATABASE=" + config.database + "; Trusted_Connection=yes;"
conn = pyodbc.connect(connconfig)
conn_read = pyodbc.connect(connconfig)
conn_insert = pyodbc.connect(connconfig)

cursor_insert = conn_insert.cursor()
cursor_read = conn_read.cursor()

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

def get_avg(min_val=None, max_val=None, med_val=None):
    # Case 1: median exists
    if is_valid_value(med_val):
        return float(med_val)

    # Case 2: min and max exist
    if is_valid_value(min_val) and is_valid_value(max_val):
        return (float(min_val) + float(max_val)) / 2

    # Case 3: only min exists
    if is_valid_value(min_val):
        return float(min_val)

    # Case 4: only max exists
    if is_valid_value(max_val):
        return float(max_val)

    # Case 5: all are None
    return float(0)

# -----------------------------------
# 2. Extract data
# -----------------------------------

salarie_range_query = """
SELECT  salary_level_key
      ,level_name
      ,min_salary
      ,max_salary
  FROM DWH.dim_salary
;
"""

df_salarie_range = pd.read_sql(salarie_range_query, conn)


query = """
SELECT p.job_id , p.max_salary , p.min_salary , p.med_salary ,  p.listed_time , p.company_id , c.state , c.city , c.country
FROM buisnessDB.postings as p
join buisnessDB.companies as c on p.company_id = c.company_id
WHERE c.city IS NOT NULL and c.city != '0' and c.city NOT LIKE '%,%'
;
"""
chunksize = 1000
chunks  = pd.read_sql(query, conn,  chunksize=chunksize)



# -----------------------------------
# 3. Insert into DWH.dim_industry
# -----------------------------------

for i, chunk in enumerate(chunks, start=1):
    for _, row in chunk.iterrows():

        if is_valid_value(row["country"]) is False:
            row["country"] = ''

        if  is_valid_value(row["state"]) is False:
            row["state"] = ''

        
        cursor_read.execute("""
                SELECT region_key
                FROM DWH.dim_region
                WHERE country = ?
                  AND state   = ?
                  AND city    = ?
        """,
        row["country"], row["state"], row["city"])
        region_key = cursor_read.fetchone()[0]

        
        listed_time = datetime.fromtimestamp(float(row["listed_time"]) / 1000).strftime("%d%m%Y")
        salary = get_avg(row["min_salary"], row["max_salary"], row["med_salary"])
        salary_key = df_salarie_range[(df_salarie_range["min_salary"] <=salary) & ((df_salarie_range["max_salary"] >= salary) | (df_salarie_range["max_salary"].isna()))].iloc[0]["salary_level_key"]


        cursor_insert.execute("""
            IF NOT EXISTS (
                SELECT 1
                FROM DWH.fact_posting
                WHERE posting_key = ?
            )
            INSERT INTO DWH.fact_posting (posting_key, company_key, date_key , region_key , salary_key  )
            VALUES (?, ? , ?, ?, ?);
        """,
        int(row["job_id"]),
        int(row["job_id"]), int(row["company_id"]), int(listed_time) , int(region_key), int(salary_key)
        )

    conn_insert.commit()
    print(f"Chunk {i} imported successfully (Rows: {len(chunk)})")


cursor_insert.close()
cursor_read.close()
conn.close()
conn_read.close()
conn_insert.close()
print("✅ fact_posting populated successfully!")
