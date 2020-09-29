import requests
import json
from json import dumps
import subprocess


  
key = 'GKNNXDF6PLD70BKF'

symbols = ["PFE","AZN","ABT","NOVN","MRK"]

hdfscmd = "hdfs dfs -put  /home/nayna01/Desktop/xdata  /nayna"

outfile='/home/nayna01/Desktop/xdata'
fo=open(outfile, 'w') 
for t in  symbols:
   f='https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={}&apikey={}'.format(t,key)
   r= requests.get(f)
   tsdata="["+r.content+"]"
   print(tsdata.encode('utf-8'))
   fo.write(tsdata.encode('utf-8'))
   print ("Record written xdata: ", t, " ", 'TimeSeriesmonthly') 
   
rval = subprocess.call(hdfscmd, shell=True)
print('returned val:', rval)
   

