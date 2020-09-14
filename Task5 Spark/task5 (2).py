from pyspark import SparkContext
#path =r'C:\Projects\Project1\Shakespeare.txt' # use your path

sc = SparkContext("local", "Bigdata task5")

#create RDD
inputrdd = sc.textFile("hdfs://localhost:9000/spark-data/Shakespeare.txt")

    
wcounts = inputrdd.flatMap(lambda x: x.split(" "))  \
          .map(lambda word: (word,1))  \
          .reduceByKey(lambda a, b:a+b)

filtered = wcounts.take(30)
print ("Output:", filtered)
wcounts.saveAsTextFile("hdfs://localhost:9000/spark-data/sp-out2.txt")


