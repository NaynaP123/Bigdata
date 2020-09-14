# Nayna Patel Big Data 2020 Task 5 :spark

from pyspark import SparkContext

#path =r'C:\Projects\Project1\Shakespeare.txt' # use your path

sc = SparkContext("local", "Bigdata task5")



#create RDD
inputrdd = sc.textFile("hdfs://localhost:9000/spark-data/Shakespeare.txt")

#collect / print rdd in a list
#liste =inputrdd.collect()
listf =inputrdd.take(20)


#print each line in list on console
for l in listf:
    print (l)


