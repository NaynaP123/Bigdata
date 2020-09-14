from kafka import KafkaProducer
import datetime as dt

Producer = KafkaProducer(bootstrap_servers='localhost:9092')

Topic='Bigdata-Task4'
listx=[]
datafile='/home/nayna01/Desktop/Shakespeare.txt'

with open(datafile, 'r') as f:
    for line in f:
        listx.append(line)
        Producer.send(Topic, line.encode('utf-8'))
        
Producer.close()
print ('Total no of recs sent to consumer from Shakespeare text:',len(listx))