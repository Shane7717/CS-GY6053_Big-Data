import sys
from pyspark import SparkContext
from decimal import Decimal
from csv import reader

sc = SparkContext()
pvRDD = sc.textFile(sys.argv[1], 1)
pvRDD = pvRDD.mapPartitions(lambda x: reader(x))
header = pvRDD.first()
pvRDD = pvRDD.filter(lambda line: line != header)

weekends = [5, 6, 12, 13, 19, 20, 26, 27]
weekendsRDD = pvRDD.filter(lambda x: int(x[1].split('-')[-1]) in weekends) \
		   .map(lambda x: (x[3], 1)) \
		   .reduceByKey(lambda x, y: x + y)

weekdaysRDD = pvRDD.filter(lambda x: int(x[1].split('-')[-1]) not in weekends) \
                   .map(lambda x: (x[3], 1)) \
                   .reduceByKey(lambda x, y: x + y)

#result1 = weekendsRDD.map(lambda x: (x[0], str(Decimal(x[1] / 8.0).quantize(Decimal("0.01"), rounding = "ROUND_HALF_UP"))))
#result2 = weekdaysRDD.map(lambda x: (x[0], str(Decimal(x[1] / 23.0).quantize(Decimal("0.01"), rounding = "ROUND_HALF_UP")))) 

result1 = weekendsRDD.map(lambda x: (x[0], '%.2f'%(x[1]/8.0)))
result2 = weekdaysRDD.map(lambda x: (x[0], '%.2f'%(x[1]/23.0))) 

answer = result1.join(result2)
answer = answer.map(lambda x: x[0] + '\t' + x[1][0] + ', ' + x[1][1]) 	    

answer.saveAsTextFile("task7.out")
    
sc.stop()
