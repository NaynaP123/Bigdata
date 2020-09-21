from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def create_df_fromrdd(rdd):
    if not rdd.isEmpty():
    	adf =  ss.createDataFrame(rdd, schema=['artist_name', 'artist_uri', \
            'artist_id', 'artist_popularity', 'artist_href', 'artist_genre1',\
            'artist_genre2','artist_genre3','artist_genre4','artist_genre5', \
            'album_name', 'album_reldate', 'album_uri', 'album_tott', \
            'album_id', 'track_name', 'track_discno', 'track_dur'])
        print ("DF SCHEMA")
        adf.printSchema() 
        print ("DF Selected fields print")                      
        adf.select('artist_name', 'artist_popularity','album_name', \
                   'album_reldate', 'track_name').show(20)
        print("TOTAL per Artists")
        adf.groupBy('artist_name').count().show
        print ("TOTAL per Albums")
        adf.groupBy('album_name').count().show
#        adf.write.mode("overwrite").saveAsTable("artist_table")      
#        adf.write.saveAsTable(name='spotifyartist', format='hive', \
#                                                           mode='append')
        
        
sc = SparkContext(appName='Twitter Kafka Spark streaming')

sc.setLogLevel("WARN")
ssc =StreamingContext(sc, 5)

ss = SparkSession.builder.appName("SparkstreamHive").   \
                  config("Spark.sql.warehouse.dir", "/user/hive/warehouse"). \
                  config("hive.metastore.uris","thrift://localhost:9083"). \
                  enableHiveSupport().getOrCreate()

ks =KafkaUtils.createDirectStream(ssc, ['Bigdata-Task9'], \
                                  {'metadata.broker.list':'localhost:9092'})
# Subscribe to 1 topic

lines = ks.map(lambda v: v[1])

tlines =lines.map(lambda artist: (artist, int(len(artist.split())), \
                                   int(len(artist))))
tlines =lines.map(lambda artist: (artist.split(" === ")))
rlines =tlines.map(lambda t: (t[0], t[1], t[2], int(t[3]), t[4],t[5], t[6], \
                         t[7], t[8], t[9], t[10], t[11], t[12], int(t[13]), \
                              t[14], t[15],int(t[16]),int(t[17]) ))
    
rlines.foreachRDD(create_df_fromrdd)

ssc.start()
ssc.awaitTermination()

