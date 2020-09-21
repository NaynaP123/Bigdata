from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils



def create_df_fromrdd(rdd):
    if not rdd.isEmpty():
        global ss
    	tdf =  ss.createDataFrame(rdd, schema=['tweet_user', 'tweet_fcount',\
                                            'tweet_created', 'tweet_text'])
        tdf.show()
        tdf.write.mode("overwrite").saveAsTable("tweet_table")
#        tweet_df.write.saveAsTable(name='default.tweets', format='hive', \
#                                                               mode='append')
        
        
sc = SparkContext(appName="SparkstreamHive")

sc.setLogLevel("WARN")
ssc =StreamingContext(sc, 5)

ss = SparkSession.builder.appName("SparkstreamHive").   \
                  config("Spark.sql.warehouse.dir", "/user/hive/warehouse"). \
                  config("hive.metastore.uris","thrift://localhost:9083"). \
                  enableHiveSupport().getOrCreate()

ks =KafkaUtils.createDirectStream(ssc, ['Bigdata-Task6'], \
                                  {'metadata.broker.list':'localhost:9092'})
# Subscribe to 1 topic

lines = ks.map(lambda v: v[1])
tlines =lines.map(lambda tweet: (tweet.split(" === ")))
rlines =tlines.map(lambda t: (t[0], int(t[1]), t[2], t[3]))

#tlines =lines.map(lambda tweet: (tweet, int(len(tweet.split())), \
 #                                  int(len(tweet))))

rlines.foreachRDD(create_df_fromrdd)

ssc.start()
ssc.awaitTermination()
