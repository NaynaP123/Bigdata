from kafka import KafkaConsumer

Topic='Bigdata-Task4'
outfile='/home/nayna01/Desktop/Shakespeareoutx.txt'
fo=open(outfile, 'a')
Consumer = KafkaConsumer(Topic,bootstrap_servers='localhost:9092',
                              auto_offset_reset='earliest',
                              consumer_timeout_ms=5000)

for line in Consumer:
    fo.write(str(line))
    fo.write("\n")
    print(str(line))
    
Consumer.close()
    
