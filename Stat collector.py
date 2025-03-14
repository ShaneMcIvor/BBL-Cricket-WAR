import pandas as pd
import sqlite3

conn = sqlite3.connect('./cricket.sqlite')

df = pd.read_sql(
    """
    SELECT SUM(Overs)*6 AS Balls, SUM(Wickets) AS Wickets
    FROM bowling
    """,
    conn)
df['SR'] = df['Balls']/df['Wickets']
print(df)
