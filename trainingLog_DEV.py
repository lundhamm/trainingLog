import pygsheets
import pandas as pd
import datetime
import sqlite3

table_name = 'backup_trainingLog'
conn = sqlite3.connect('trainingLog.db')
query = query = f'Create table if not Exists {table_name} ([partitionKey] date,  [date] date, [excercise] text, [weight] real, [set] real, [reps] real, [comment] text)'
conn.execute(query)
cursor = conn.cursor()

selectRecentQuery = f"select * from (select *, ROW_NUMBER() OVER (PARTITION BY [date],[excercise], [weight], [set], [reps], [comment] ORDER BY partitionKey desc) rowId from {table_name}) t where rowId > 1 "
cursor.execute(selectRecentQuery)
results = cursor.fetchall()

d = {}
for i, row in enumerate(results):
    if i == 0:
        d['partitionKey'] = []
        d['date'] = []
        d['excercise'] = []
        d['weight'] = []
        d['set'] = []
        d['reps'] = []
        d['comment'] = []
    else:
        currRow = row[0:7]

        d['partitionKey'] += [currRow[0]]
        d['date'] += [currRow[1]]
        d['excercise'] += [currRow[2]]
        d['weight'] += [currRow[3]]
        d['set'] += [currRow[4]]
        d['reps'] += [currRow[5]]
        d['comment'] += [currRow[6]]

df = pd.DataFrame(data=d)

print(df)
