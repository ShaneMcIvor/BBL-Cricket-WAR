import pandas as pd
import sqlite3

conn = sqlite3.connect('./cricket.sqlite')

batting = pd.read_excel('BBL24.xlsx', sheet_name = "Batting")
batting.to_sql('batting', conn, index = False, if_exists='replace')


df = pd.read_sql(
    """
    SELECT WicketNo, COUNT(*) AS TimesWicketFell
    FROM batting
    WHERE WicketNo IS NOT NULL
    GROUP BY WicketNo
    """,
    conn)
print(df)
