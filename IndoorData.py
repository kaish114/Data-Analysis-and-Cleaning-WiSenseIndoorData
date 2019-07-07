import sys

def progressbar(it, prefix="", size=60, file=sys.stdout):
    count = len(it)
    def show(j):
        x = int(size*j/count)
        file.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), j, count))
        file.flush()        
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    file.write("\n")
    file.flush()

import time


# Above Function is used to create Progress bar



#!/usr/bin/env python
# coding: utf-8

# # WiSenseIndoorData, Cleaning of Data, Finding Error % in each node

# # 1. Importing Libraries
import numpy as np
import pandas as pd


# # 2. Reading WiSenseData

#Reading the Indoor dataset
dff = pd.read_csv('WiSenseIndoorData.csv' , header = None)  



# # 3. Renaming the Columns

df = dff.rename(columns={0: 'timeStamp', 1: 'nodeAddress' , 2: 'packteID', 3: 'nodeRSSI', 4: 'nodeVolt', 5: 'temperature1', 6: 'temperature2', 7: 'temperature3', 8: 'pressure', 9: 'luminosity' , 10: 'rainfall', 11: 'solarPanelVolt', 12: 'solarPanelBattVolt', 13: 'solarPanelCurr'})


#Copying the original dataset ('df') into data1
data1 = df.copy() 



#Converting datatype of 'timeStamp' to datetime type
data1['timeStamp'] = pd.to_datetime(data1['timeStamp'])  



# Now We will create two new columns in our Dataset namely, 'temp1_changed', 'temp2_changed' 'temp3_changed' and 'pressure_changed'
# These column will contain value '1' if corresponding values are changed changed else it will contain 0
data1['temp1_changed'] = 0
data1['temp2_changed'] = 0
data1['temp3_changed'] = 0
data1['pressure_changed'] = 0



# Following Scripts will deal with first value of each node if it is outlier

#1. We'll just check if first value of each node for a particular column is outlier (i.e temperature > 100 or temperature < 0), if it is outlier then we'll change its value to next row value


#from tqdm import tqdm_notebook

nodes = data1['nodeAddress'].unique() # this line will create an array having total unique nodes

print('Checking Outlier for temperature1')
for n in progressbar(nodes, "Processing records for Outlier "):
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'nodeAddress'] == n):
            val0 = float(data1.loc[i,'temperature1'])
            if(val0 < 0 or val0 > 100):
                data1.loc[i,'temperature1'] = data1.loc[i+1,'temperature1']
                print('Outlier Found at', i , 'for node' , n)
                break
            else:
                break

print('Checking Outlier for temperature2') 
for n in progressbar(nodes, "Processing records for Outlier "):               
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'nodeAddress'] == n):
            val0 = float(data1.loc[i,'temperature2'])
            if(val0 < 0 or val0 > 100):
                data1.loc[i,'temperature2'] = data1.loc[i+1,'temperature2']
                print('Outlier Found at',i, 'for node' , n)
                break
            else:
                break
                
                
print('Checking Outlier for temperature3')
for n in progressbar(nodes, "Processing records for Outlier "):                
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'nodeAddress'] == n):
            val0 = float(data1.loc[i,'temperature3'])
            if(val0 < 0 or val0 > 100):
                data1.loc[i,'temperature3'] = data1.loc[i+1,'temperature3']
                print('Outlier Found at',i, 'for node' , n)
                break
            else:
                break
                
                
print('Checking Outlier for pressure')
for n in progressbar(nodes, "Processing records for Outlier "):
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'nodeAddress'] == n):
            val0 = float(data1.loc[i,'pressure'])
            if(val0 < 750 or val0 > 1000):
                data1.loc[i,'pressure'] = data1.loc[i+1,'pressure']
                print('Outlier Found at',i, 'for node' , n)
                break
            else:
                break

                





# # Following is the function to clean 'temp1', 'temp2', 'temp3' and 'pressure'


nodes = data1['nodeAddress'].unique() # this line will create an array having total unique nodes


#Function to clean 'temperature1'


def temperature1_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'temperature1'])
            time0 = (df.loc[i,'timeStamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'temperature1'])
                time1 = (df.loc[j , 'timeStamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'temperature1'] = val0
                  df.loc[j, 'temp1_changed'] = 1
                  k = j
                  break
                elif((val1) > 100  and minutes > 30.0):
                  df.loc[j,'temperature1'] = 'NaN'
                  k = j
                  break
                
                elif((val1) < 0  and minutes > 30.0):
                  df.loc[j,'temperature1'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                    
                 


#Function to clean 'temperature2'

def temperature2_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'temperature2'])
            time0 = (df.loc[i,'timeStamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'temperature2'])
                time1 = (df.loc[j , 'timeStamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'temperature2'] = val0
                  df.loc[j, 'temp2_changed'] = 1
                  k = j
                  break
                elif(((val1) > 100 or (val1) < 0 ) and minutes > 30.0):
                  df.loc[j,'temperature2'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                    
                 

#Function to clean 'temperature3'

def temperature3_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'temperature3'])
            time0 = (df.loc[i,'timeStamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'temperature3'])
                time1 = (df.loc[j , 'timeStamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'temperature3'] = val0
                  df.loc[j, 'temp3_changed'] = 1
                  k = j
                  break
                elif(((val1) > 100 or (val1) < 0 ) and minutes > 30.0):
                  df.loc[j,'temperature3'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                    
                    
                   
# Function to clean 'pressure'

def pressure_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'pressure'])
            time0 = (df.loc[i,'timeStamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'pressure'])
                time1 = (df.loc[j , 'timeStamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'pressure'] = val0
                  df.loc[j, 'pressure_changed'] = 1
                  k = j
                  break
                elif(((val1) > 1000 or (val1) < 750 ) and minutes > 30.0):
                  df.loc[j,'pressure'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break


print('Cleaning Temperature1 Column')
temperature1_clean(data1)

print('Cleaning Temperature2 Column')
temperature2_clean(data1)

print('Cleaning Temperature3 Column')
temperature3_clean(data1)

print('Cleaning Pressure Column')
pressure_clean(data1)


data1.to_csv('kaish.csv')
