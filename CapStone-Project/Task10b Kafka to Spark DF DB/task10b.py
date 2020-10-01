from pyspark import SparkContext
from pyspark.sql import SparkSession
#from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition



#Topic=['Task10-Overview']
Topic=['Task10-DlyPrices','Task10-Overview','Task10-IncomeS','Task10-BalanceS']
sc = SparkContext(appName="CapStoneProject")
ssc = StreamingContext(sc, 5)
ss = SparkSession.builder.appName("CapStone")     \
                 .master("local")     \
                 .config("spark.mongodb.output.uri", \
                        "mongodb://127.0.0.1:27017/mydb.test") \
                 .config("spark.driver.extraClassPath",   \
                                "/usr/local/opts/spark-2.4.4/jars/") \
                 .getOrCreate()  
                             


#print RDD       
def handler(message):
    records = message.collect()
    for record in records:
        print(record[1])
        
#create DF /Databases 
def CreateDfforjson(message):
    global wcollection
    if not message.isEmpty():
      if wtopic == 'Task10-Overview':
         wcollection="Overviewx"
      elif wtopic== 'Task10-IncomeS':
         wcollection = "IncomeSx"
      else:
         wcollection = "BalanceSx"
      print("******************", wtopic, "***" , wcollection)
      jdata = ss.read.json(message) 
      jdata.show()
      jdata.printSchema()    
      jdata.write.format("com.mongodb.spark.sql.DefaultSource")  \
           .mode("overwrite")             \
           .option("uri","mongodb://127.0.0.1:27017/mydb.{}".format(wcollection)) \
           .option("database", "mydb")   \
           .option("collection", "{}".format(wcollection))   \
           .save()
     
    
def CreateDffortuple(rdd):
    if not rdd.isEmpty(): 
       tdf =  ss.createDataFrame(rdd, schema=['Symbol','Date', \
                    'High','Low','Open', 'Close', 'Volume','AdjClose'])
       tdf.show()
       tdf.write.format("com.mongodb.spark.sql.DefaultSource")  \
            .mode("overwrite")             \
            .option("uri","mongodb://127.0.0.1:27017/mydb.DlyPricesx" ) \
            .option("database", "mydb")   \
            .option("collection", "DlyPricesx")   \
            .save() 
    
   
def streaming():
    global wtopic
    for i in range (0, len(Topic)):     
      print(Topic[i])  
      fromOffsets = {TopicAndPartition(Topic[i], 0): long(0)}
      kafkaParams = {"metadata.broker.list":'localhost:9092'}
      ks =KafkaUtils.createDirectStream(ssc, [Topic[i]], \
                                            kafkaParams, \
                                            fromOffsets)
      
      ks.foreachRDD(handler)
      if  (Topic[i] ==  'Task10-DlyPrices'):
          print("prices")
          lines = ks.map(lambda v: v[1])  
          tlines =lines.map(lambda prices: (prices.split(",")))
          rlines =tlines.map(lambda t: (t[0], str(t[1]), t[2], t[3], t[4], \
                                      t[5],t[6],t[7]))
          rlines.foreachRDD(CreateDffortuple)
      else: 
          wtopic = Topic[i]
          print("other ", wtopic)
          lines = ks.map(lambda v: v[1]) 
          lines.foreachRDD(CreateDfforjson)
#          
                       
if __name__ == "__main__":    
   streaming()
   ssc.start()
   ssc.awaitTermination()

   



