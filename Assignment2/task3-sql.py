import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import string
from csv import reader
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3-sql").getOrCreate()

opening = spark.read.format('csv') \
    .options(header='true', inferschema='true') \
    .load(sys.argv[1])

opening.createOrReplaceTempView("opening")

result = spark.sql("select precinct, cast(sum(amount_due) as decimal(10, 2)) as num1, cast(avg(amount_due) as decimal(10, 2)) as num2 \
		from opening group by precinct")

result.select(format_string('%d\t%s, %s',result.precinct, result.num1, result.num2)).write.save("task3-sql.out", format="text")
