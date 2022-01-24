import sys
from pyspark import SparkContext
from decimal import Decimal
from csv import reader

sc = SparkContext()
pvRDD = sc.textFile(sys.argv[1], 1)
pvRDD = pvRDD.mapPartitions(lambda x: reader(x))
header = pvRDD.first()
pvRDD = pvRDD.filter(lambda line: line != header)

pair = pvRDD.map(lambda line: ('NY', 1) if line[-6]=='NY' else ('Other', 1))
result = pair.reduceByKey(lambda x, y: x + y)
result = result.map(lambda line: line[0] + '\t' + str(line[1]))

result.saveAsTextFile("task4.out")
    
sc.stop()
