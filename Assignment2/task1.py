import sys
from csv import reader
from pyspark import SparkContext


sc = SparkContext()
parking_violations = sc.textFile(sys.argv[1], 1)
open_violations = sc.textFile(sys.argv[2], 1)

parking_violations = parking_violations.mapPartitions(lambda x: reader(x))
open_violations = open_violations.mapPartitions(lambda x: reader(x))

pvRDD = parking_violations.map(lambda x: (x[0], x[3] + ', ' + x[-6] + ', ' + x[2] + ', ' + x[1])) 

#pvRDD = parking_violations.map(lambda line: (line.split(',')[0], 
#                 line.split(',')[3] + ', ' +
#                 line.split(',')[-6] + ', ' +
#                 line.split(',')[2] + ', ' +
#                 line.split(',')[1]))
 
#ovRDD = open_violations.map(lambda line: (line.split(',')[0], None))
ovRDD = open_violations.map(lambda x: (x[0], None))

pvRDD = pvRDD.subtractByKey(ovRDD)
    
result = pvRDD.map(lambda pair: pair[0] + '\t' + pair[1])  
     
result.saveAsTextFile("task1.out")
    
sc.stop()
