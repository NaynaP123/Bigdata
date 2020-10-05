from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json, col



sc = SparkContext(appName="Task10a2 Timeseries monthly")
ssc = StreamingContext(sc, 5) 
    
ss = SparkSession.builder.appName("CapStone")     \
                 .master("local")     \
                 .config("spark.mongodb.output.uri", \
                        "mongodb://127.0.0.1:27017/mydb.test") \
                 .config("spark.driver.extraClassPath",   \
                                "/usr/local/opts/spark-2.4.4/jars/") \
                 .getOrCreate()  
                             
# Create data frame
                        
df = ss.read.json("hdfs://localhost:9000/nayna/Mthlyseries")

df.show()
df.printSchema()   

wcollection="mthlyseries"  
df.write.format("com.mongodb.spark.sql.DefaultSource")  \
           .mode("overwrite")             \
           .option("uri","mongodb://127.0.0.1:27017/mydb.{}".format(wcollection)) \
           .option("database", "mydb")   \
           .option("collection", "{}".format(wcollection))   \
           .save()
ssc.start()
ssc.awaitTermination()
