import sys
from pyspark import SparkContext
from decimal import Decimal
from csv import reader

sc = SparkContext()
ovRDD = sc.textFile(sys.argv[1])
ovRDD = ovRDD.mapPartitions(lambda x: reader(x))
header = ovRDD.first()

pair = ovRDD.filter(lambda line: line != header) \
    	    .map(lambda line: (line[5], (float(line[-6]), 1))) \
	    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
       	       
result = pair.map(lambda line: line[0] + '\t' + 
		str(Decimal(line[1][0]).quantize(Decimal("0.01"), rounding = "ROUND_HALF_UP")) + ', ' +
		str(Decimal(line[1][0] / line[1][1]).quantize(Decimal("0.01"), rounding = "ROUND_HALF_UP"))) 

#	      .mapValues(list) \
#     	      .map(lambda line: line[0] + '\t' + str(len(line[1])))
#		str(Decimal(sum(line[1])).quantize(Decimal("0.01"), rounding = "ROUND_HALF_UP")) + ', ')
#        	str(Decimal(sum(line[1]) / len(line[1])).quantize(Decimal("0.01"), rounding = "ROUND_HALF_UP")))


result.saveAsTextFile("task3.out")
    
sc.stop()
