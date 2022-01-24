import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import string
from csv import reader
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task1-sql").getOrCreate()

parking = spark.read.format('csv') \
    .options(header='true', inferschema='true') \
    .load(sys.argv[1]) 

opening = spark.read.format('csv') \
    .options(header='true', inferschema='true') \
    .load(sys.argv[2])

parking.createOrReplaceTempView("parking") 
opening.createOrReplaceTempView("opening")

result = spark.sql("SELECT P.summons_number, P.violation_county, P.registration_state, P.violation_code, P.issue_date \
FROM parking P LEFT OUTER JOIN opening O ON P.summons_number = O.summons_number WHERE O.summons_number is NULL")

result.select(format_string('%d\t%s, %s, %d, %s',result.summons_number,result.violation_county,result.registration_state,result.violation_code, \
date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")
