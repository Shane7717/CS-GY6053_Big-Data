import sys
from pyspark import SparkContext
#from decimal import Decimal
from csv import reader

sc = SparkContext()
pvRDD = sc.textFile(sys.argv[1])
pvRDD = pvRDD.mapPartitions(lambda x: reader(x))
header = pvRDD.first()
pvRDD = pvRDD.filter(lambda line: line != header)

pair = pvRDD.map(lambda line: (line[-2], 1)) \
	    .reduceByKey(lambda x, y: x + y)

result = pair.sortBy(lambda x: (-x[1],x[0]))
ans = result.take(10)
ans = sc.parallelize(ans)
ans = ans.map(lambda x: x[0] + '\t' + str(x[1]))

ans.saveAsTextFile("task6.out")
    
sc.stop()
