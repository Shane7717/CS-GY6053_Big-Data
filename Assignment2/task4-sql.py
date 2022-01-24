import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import string
from csv import reader
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task4-sql").getOrCreate()

parking = spark.read.format('csv') \
    .options(header='true', inferschema='true') \
    .load(sys.argv[1])

parking.createOrReplaceTempView("parking")

result = spark.sql("SELECT registration_state, count(registration_state) AS total FROM \
(SELECT CASE WHEN registration_state = 'NY' THEN 'NY' ELSE 'Other' END AS registration_state FROM parking) GROUP BY registration_state")

result.select(format_string('%s\t%d',result.registration_state, result.total)).write.save("task4-sql.out", format="text")
