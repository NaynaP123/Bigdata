import requests
import json
from json import dumps
import subprocess


  
key = 'GKNNXDF6PLD70BKF'

symbols = ["PFE","AZN","ABT","NOVN","MRK"]

hdfscmd = "hdfs dfs -put  /home/nayna01/Desktop/Mthlyseries  /nayna"
cols4=[]
values4=[]

outfile='/home/nayna01/Desktop/Mthlyseries'
fo=open(outfile, 'a') 
for t in  symbols:
   count=0
   f='https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={}&apikey={}'.format(t,key)
   r= requests.get(f)
   isdata=r.json()
   cols = isdata.keys() 
   values = isdata.values() 
   cols2 = values[0].keys()
   values2 = values[0].values()
   cols3 = values[1].keys()
   values3 = values[1].values() 
   ar={}
   ar['1_Symbol']=t 
   ar['2_Description']="Monthly Price series"
   ar['3_DateLastRefreshed']=values3[2]
   for i in range (0, len(cols2)):       
       wdate =cols2[i]
       wprices=(values2[i].values()) 
       wcols=(values2[i].keys()) 
#       print (i, wdate, " ", t)
       ar['4-Date']=wdate
       ar['9_Volume']=wprices[0]
       ar['8_Close']=wprices[1]
       ar['5_High']=wprices[2]
       ar['7_Open']=wprices[3]
       ar['6_Low']=wprices[4]
       ardata=json.dumps(ar, sort_keys=True)
       print(ardata)
       fo.write(ardata.encode('ascii'))
       fo.write("\n")
       count=count+1
   print ("Record written finaljdata: ", count, " ", t) 
       
   
rval = subprocess.call(hdfscmd, shell=True)
print('returned val:', rval)
   

