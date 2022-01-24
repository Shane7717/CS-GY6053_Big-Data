import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import string
from csv import reader
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task5-sql").getOrCreate()

parking = spark.read.format('csv') \
    .options(header='true', inferschema='true') \
    .load(sys.argv[1])

parking.createOrReplaceTempView("parking")

#result = spark.sql("SELECT vehicle_make, MAX(num) AS maximum \
#	FROM (SELECT vehicle_make, count(*) AS num FROM parking GROUP BY vehicle_make) AS sub") 

result = spark.sql("SELECT vehicle_make, count(*) AS num FROM parking GROUP BY vehicle_make \
		ORDER BY num DESC LIMIT 1")

result.select(format_string('%s\t%d',result.vehicle_make, result.num)).write.save("task5-sql.out", format="text")
