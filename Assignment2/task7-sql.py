import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import string
from csv import reader

from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task7-sql").getOrCreate()

parking = spark.read.format('csv') \
        .options(header='true', inferschema='true') \
        .load(sys.argv[1])

parking.createOrReplaceTempView("parking")

result = spark.sql("SELECT violation_county, CAST(SUM(weekend) AS float) / 8 AS weekend_average, CAST(SUM(weekday) AS float) / 23 AS weekday_average FROM \
((SELECT violation_county, 1 AS weekend, 0 AS weekday FROM parking WHERE DAY(parking.issue_date) IN (5, 6, 12, 13, 19, 20, 26, 27)) UNION ALL \
(SELECT violation_county, 0 AS weekend, 1 AS weekday FROM parking WHERE DAY(parking.issue_date) NOT IN (5, 6, 12, 13, 19, 20, 26, 27))) GROUP BY violation_county")

result.select(format_string('%s\t%.2f, %.2f',result.violation_county,result.weekend_average,result.weekday_average)).write.save("task7-sql.out", format="text", nullValue="null")
