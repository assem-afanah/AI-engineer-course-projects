import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine , types
import pyodbc
import config

# -----------------------------
# 1. SQL Server Connection
# -----------------------------
conn = pyodbc.connect(
    "DRIVER={" + config.driver + "};"
    "SERVER=" + config.server + ";"
    "DATABASE=" + config.database + ";"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# -----------------------------
# 2. generate Date data using pandas
# -----------------------------

# Define date range
start_date = "2023-01-01"
end_date   = "2029-12-31"

# Create date range
dates = pd.date_range(start=start_date, end=end_date)

# Build DimDate table
dim_date = pd.DataFrame({
    "key": dates.strftime("%d%m%Y").astype(int), 
    "date_key": dates.strftime("%d%m%Y").astype(int),   
    "DayOfWeek": dates.strftime("%A"),
    "MonthNumber": dates.month,
    "YearNumber": dates.year
})

# -----------------------------
# 3. insert data into DimDate
# -----------------------------
cursor.executemany("""
        IF NOT EXISTS (
            SELECT 1
            FROM DWH.dim_date
            WHERE date_key = ?
        )
        INSERT INTO DWH.dim_date (date_key, DayOfWeek, MonthNumber, YearNumber)
        VALUES (?, ? , ?, ?)
    """, 
    dim_date.values.tolist()
)
conn.commit()
cursor.close()
conn.close()

print("✅ Population of date dim table is completed!")