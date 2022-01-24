import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import string
from csv import reader
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2-sql").getOrCreate()

opening = spark.read.format('csv') \
    .options(header='true', inferschema='true') \
    .load(sys.argv[1])

opening.createOrReplaceTempView("opening")

result = spark.sql("select violation, count(violation) total from opening group by violation")
 
result.select(format_string('%s\t%d',result.violation, result.total)).write.save("task2-sql.out", format="text")
