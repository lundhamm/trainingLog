# Run to update backup db and update cleaned data in cloud.


import pygsheets
import pandas as pd
import datetime
import sqlite3

now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
numRollbacks = 0
#authorization
gc = pygsheets.authorize(service_file='/home/perlundhammar/Skrivbord/trainingLog/traininglog-creds.json')

sh = gc.open('trainingLog')


d = {}
for i, row in enumerate(sh[0]):
    if i == 0:
        header = row[0:6]
        d['partitionKey'] = []
        d[row[0]] = []
        d[row[1]] = []
        d[row[2]] = []
        d[row[3]] = []
        d[row[4]] = []
        d[row[5]] = []
    else:
        currRow = row[0:6]
        # Filling out the date column
        if currRow[0] != '':
            currDate = currRow[0]
        elif currRow[0] == '':
            currRow[0] = currDate

        # Filling out the excercise column
        if currRow[1] != '':
            currEx = currRow[1]
        elif currRow[1] == '':
            currRow[1] = currEx

        d['partitionKey'] = now
        d[header[0]] += [currRow[0]]
        d[header[1]] += [currRow[1]]
        d[header[2]] += [float(currRow[2].replace(',','.'))]
        d[header[3]] += [float(currRow[3].replace(',','.'))]
        d[header[4]] += [float(currRow[4].replace(',','.'))]
        d[header[5]] += [currRow[5]]

df = pd.DataFrame(data=d)

table_name = 'backup_trainingLog'
conn = sqlite3.connect('trainingLog.db')
query = query = f'Create table if not Exists {table_name} ([partitionKey] date,  [date] date, [excercise] text, [weight] real, [set] real, [reps] real, [comment] text)'
conn.execute(query)
cursor = conn.cursor()
numInsertQuery = f"select MAX(rowId) from (select *, ROW_NUMBER() OVER (PARTITION BY [date],[excercise], [weight], [set], [reps], [comment]) rowId from {table_name}) t "
cursor.execute(numInsertQuery)
results = cursor.fetchall()

try:
    if results[0][0]>numRollbacks:

        selectRecentQuery = f"select * from (select *, ROW_NUMBER() OVER (PARTITION BY [date],[excercise], [weight], [set], [reps], [comment] ORDER BY partitionKey asc) rowId from {table_name}) t where rowId > 1 "
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
                d['weight'] += [str(currRow[3]).replace('.',',')]
                d['set'] += [str(currRow[4]).replace('.',',')]
                d['reps'] += [str(currRow[5]).replace('.',',')]
                d['comment'] += [currRow[6]]

        df = pd.DataFrame(data=d)

        df.to_sql(table_name,conn,if_exists='replace',index=False)

    else:
        df.to_sql(table_name,conn,if_exists='append',index=False)
except:
    df.to_sql(table_name,conn,if_exists='append',index=False)




selectRecentQuery = f"select * from (select *, ROW_NUMBER() OVER (PARTITION BY [date],[excercise], [weight], [set], [reps], [comment] ORDER BY partitionKey desc) rowId from {table_name}) t where rowId = 1 "
cursor.execute(selectRecentQuery)
results = cursor.fetchall()

d = {}
for i, row in enumerate(results):
    if row[-1] == 1:
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
            d['weight'] += [str(currRow[3]).replace('.',',')]
            d['set'] += [str(currRow[4]).replace('.',',')]
            d['reps'] += [str(currRow[5]).replace('.',',')]
            d['comment'] += [currRow[6]]

df = pd.DataFrame(data=d)

conn.commit()
conn.close()

#df = df.drop(labels=['partitionKey'], axis = 1)
#upperRow = pd.DataFrame({'updated':[now]})
if sh[1].title == 'trainingLogCleaned':
    sh[1].set_dataframe(df,(1,1))
    #sh[1].set_dataframe(upperRow,(1,2))
