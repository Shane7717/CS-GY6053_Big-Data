import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
ovRDD = sc.textFile(sys.argv[1], 1)
ovRDD = ovRDD.mapPartitions(lambda x: reader(x))

header = ovRDD.first()

result = ovRDD.filter(lambda line: line != header) \
	      .map(lambda line: (line[7], 1)) \
     	      .reduceByKey(lambda x, y: x + y) \
	      .map(lambda pair: pair[0] + '\t' + str(pair[1])) 
	     
result.saveAsTextFile("task2.out")
    
sc.stop()
