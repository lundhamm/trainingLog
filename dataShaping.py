import pygsheets
import pandas as pd
import datetime
import sqlite3
import numpy as np
#authorization
gc = pygsheets.authorize(service_file='/home/perlundhammar/Skrivbord/trainingLog/traininglog-creds.json')

sh = gc.open('trainingLog')

if sh[1].title == 'trainingLogCleaned':
    d = {}
    for i, row in enumerate(sh[1]):
        if i == 0:
            header = row[0:7]
            d[row[0]] = []
            d[row[1]] = []
            d[row[2]] = []
            d[row[3]] = []
            d[row[4]] = []
            d[row[5]] = []
            d[row[6]] = []
        else:
            currRow = row[0:7]
            d[header[0]] += [currRow[0]]
            d[header[1]] += [currRow[1]]
            d[header[2]] += [currRow[2]]
            d[header[3]] += [float(currRow[3].replace(',','.'))]
            d[header[4]] += [float(currRow[4].replace(',','.'))]
            d[header[5]] += [float(currRow[5].replace(',','.'))]
            d[header[6]] += [currRow[6]]

    df = pd.DataFrame(data=d)



lastUpdated = df['partitionKey'][0]
dates = np.unique(df['date'].values)
excercises = np.unique(df['excercise'].values)


d = {'date':[]}
d2 = {'date':[]}
for e in excercises:
    d['Mean:'+e] = []
    d['Max:'+e] = []
    d['Min:'+e] = []
    d2['Max:'+e] = []
for date in dates:
    d['date'] += [date]
    d2['date'] += [date]
    for i,ex in enumerate(excercises):
        weights = df['weight'].loc[df['date']==date].loc[df['excercise']==ex].values
        if weights.size > 0:

            meanW = str(np.mean(weights)).replace('.',',')
            maxW = str(np.max(weights)).replace('.',',')
            minW = str(np.min(weights)).replace('.',',')
        else:
            meanW = ''
            maxW  = ''
            minW = ''
        d['Mean:'+ex] += [meanW]
        d['Max:'+ex] += [maxW]
        d['Min:'+ex] += [minW]
        d2['Max:'+ex] += [maxW]

dfS1 = pd.DataFrame(data=d)
dfL1 = pd.DataFrame(data=d2)

d = {'date':[]}
d2 = {'date':[]}
for e in excercises:
    d['Mean:'+e] = []
    d['Max:'+e] = []
    d['Min:'+e] = []
    d2['Max:'+e] = []

for date in dates:
    d['date'] += [date]
    d2['date'] += [date]
    for i,ex in enumerate(excercises):
        weights = df['reps'].loc[df['date']==date].loc[df['excercise']==ex].values
        if weights.size > 0:

            meanW = str(np.mean(weights)).replace('.',',')
            maxW = str(np.max(weights)).replace('.',',')
            minW = str(np.min(weights)).replace('.',',')
        else:
            meanW = ''
            maxW  = ''
            minW = ''
        d['Mean:'+ex] += [meanW]
        d['Max:'+ex] += [maxW]
        d['Min:'+ex] += [minW]
        d2['Max:'+ex] += [maxW]

dfS2 = pd.DataFrame(data=d)
dfL2 = pd.DataFrame(data=d2)
d = {'date':[]}
for e in excercises:
    d['Max:'+e] = []

for date in dates:
    d['date'] += [date]
    for i,ex in enumerate(excercises):
        weights = df['set'].loc[df['date']==date].loc[df['excercise']==ex].values
        if weights.size > 0:
            meanW = str(np.mean(weights)).replace('.',',')
            maxW = str(np.max(weights)).replace('.',',')
        else:
            maxW  = ''
        d['Max:'+ex] += [maxW]

dfS3 = pd.DataFrame(data=d)

lW = dfL1[dfL1.tail(1)!=''].tail(1).dropna(how='all',axis=1)
lR = dfL2[dfL2.tail(1)!=''].tail(1).dropna(how='all',axis=1)
lS = dfS3[dfS3.tail(1)!=''].tail(1).dropna(how='all',axis=1)


d = {lW['date'].values[0]:['weight','reps','set']}
for i,col in enumerate(lW.columns):
    if i>0:
        ex = col[4:]
        d[ex] = [lW[col].values[0],lR[col].values[0],lS[col].values[0]]

dfL = pd.DataFrame(data=d)


dfL1.drop(dfL1.tail(1).index,inplace=True)
dfL2.drop(dfL2.tail(1).index,inplace=True)
dfS3.drop(dfS3.tail(1).index,inplace=True)
lW = dfL1[dfL1.tail(1)!=''].tail(1).dropna(how='all',axis=1)
lR = dfL2[dfL2.tail(1)!=''].tail(1).dropna(how='all',axis=1)
lS = dfS3[dfS3.tail(1)!=''].tail(1).dropna(how='all',axis=1)


d = {lW['date'].values[0]:['weight','reps','set']}
for i,col in enumerate(lW.columns):
    if i>0:
        ex = col[4:]
        d[ex] = [lW[col].values[0],lR[col].values[0],lS[col].values[0]]

dfLL = pd.DataFrame(data=d)

print(excercises)

# BÄNK
bank = dfS1[['date','Max:Bänkpress','Min:Bänkpress','Mean:Bänkpress']].replace('', np.nan).dropna(subset=['Max:Bänkpress','Min:Bänkpress','Mean:Bänkpress'])

# Marklyft
mark = dfS1[['date','Max:Marklyft','Min:Marklyft','Mean:Marklyft']].replace('', np.nan).dropna(subset=['Max:Marklyft','Min:Marklyft','Mean:Marklyft'])

# Styrkevändning
styrkelyft = dfS1[['date','Max:Styrkevändning','Min:Styrkevändning','Mean:Styrkevändning']].replace('', np.nan).dropna(subset=['Max:Styrkevändning','Min:Styrkevändning','Mean:Styrkevändning'])

# Militärpress
milit = dfS1[['date','Max:Militärpress','Min:Militärpress','Mean:Militärpress']].replace('', np.nan).dropna(subset=['Max:Militärpress','Min:Militärpress','Mean:Militärpress'])

# Knäböj
kna = dfS1[['date','Max:Knäböj','Min:Knäböj','Mean:Knäböj']].replace('', np.nan).dropna(subset=['Max:Knäböj','Min:Knäböj','Mean:Knäböj'])

#print('Bänkpress')
#print(bank)

#print('Marklyft')
#print(mark)

#print('Styrkevändning')
#print(styrkelyft)

#print('Militärpress')
#print(milit)

#print('Knäböj')
#print(kna)




sh[2].set_dataframe(dfL,(1,1))
sh[2].set_dataframe(dfLL,(1,5))

sh[2].set_dataframe(bank,(15,1))
sh[2].set_dataframe(mark,(15,5))
sh[2].set_dataframe(styrkelyft,(15,9))
sh[2].set_dataframe(milit,(15,15))
sh[2].set_dataframe(kna,(15,19))
