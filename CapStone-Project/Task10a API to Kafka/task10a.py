from kafka import KafkaProducer
import requests
import json
import pandas_datareader.data as web
import datetime as dt


Overview='Task10-Overview'
IncomeS='Task10-IncomeS'
BalanceS='Task10-BalanceS'
DlyPrices='Task10-DlyPrices'

Producer = KafkaProducer(bootstrap_servers='localhost:9092')

  
key = 'GKNNXDF6PLD70BKF'
key2 = 'LHXEXZGDJ7NOD816'
symbols = ["PFE","AZN","ABT","NOVN","MRK"]


    
for t in  symbols: 
   a='https://www.alphavantage.co/query?function=OVERVIEW&symbol={}&apikey={}'.format(t,key)
   r = requests.get(a)   
   ovdata=r.content
#   print(ovdata)
#   print("***********")
   print ("Message sent: ", t, " ", 'Overview') 
   Producer.send(Overview, ovdata.encode('utf-8'))
 
         
   b='https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={}&apikey={}'.format(t,key2)
   r = requests.get(b)   
   isdata=r.json()
   ar={}
   print(len(isdata['annualReports']))
   ar['Symbol']=t 
   ar['Description']="Income Statement"
   for i in range (0, len(isdata['annualReports'])):
       ar['fiscalDateEnd']=isdata['annualReports'][i]['fiscalDateEnding']
       ar['grossProfit']=isdata['annualReports'][i]['grossProfit']
       ar['incomeBeforetax']=isdata['annualReports'][i]['incomeBeforeTax']
       ar['netIncome']=isdata['annualReports'][i]['netIncome']
       ar['reportedCurrency']=isdata['annualReports'][i]['reportedCurrency']
       ardata=json.dumps(ar)
#       print(ardata)
#       print("************")
       print ("Message sent: ", ar['Symbol'], " ", ar['Description']) 
       Producer.send(IncomeS, ardata.encode('ascii'))
    
   c='https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={}&apikey={}'.format(t,key)
   r = requests.get(c)
   isdatax=r.json()
   bs={}
   print(len(isdatax['annualReports']))
   bs['Symbol']=t 
   bs['Description']="Balance Sheet"
   for i in range (0, len(isdatax['annualReports'])):
       bs['fiscalDateEnd']=isdatax['annualReports'][i]['fiscalDateEnding']
       bs['reportedCurrency']=isdatax['annualReports'][i]['reportedCurrency']
       bs['totalAssets']=isdatax['annualReports'][i]['totalAssets']
       bs['totalLiabilities']=isdatax['annualReports'][i]['totalLiabilities']
       bs['cash']=isdatax['annualReports'][i]['cash']
       bs['shortTermDebt']=isdatax['annualReports'][i]['shortTermDebt']
       bs['longTermDebt']=isdatax['annualReports'][i]['longTermDebt']
       bs['accountsPayable']=isdatax['annualReports'][i]['accountsPayable']
       bsdata=json.dumps(bs)
#       print(bsdata)
#      print("************")
       print ("Message sent: ", bs['Symbol'], " ", bs['Description'])      
       Producer.send(BalanceS, bsdata.encode('ascii'))
           
   start=dt.datetime(2020,7,1)
   end=dt.datetime(2020,8,31)
   df=web.DataReader(t,'yahoo',start,end)
   print(df.head(10))
  
   for i in range (0,len(df)):
       prdata=t+","+str(df.index[i])+","+str(df['High'][i])+","+str(df['Low'][i])+","+ \
              str(df['Open'][i])+","+str(df['Close'][i])+","+str(df['Volume'][i])+","+ \
              str(df['Adj Close'][i])
#      print(prdata)
#      print("************")
       print ("Message sent: ", t, "  DlyPrices")      
       Producer.send(DlyPrices, prdata)
   
Producer.close

