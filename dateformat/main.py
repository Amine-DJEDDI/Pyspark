from pyspark.sql import *

from pyspark.sql.functions import *
import pyspark.sql.functions as F
from os.path import abspath
from pyspark.sql import Row
import pandas as pd

# warehouse_location points to the default location for managed databases and tables
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, DateType

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .getOrCreate()


xlsxfile = "/home/amine/Desktop/substringy.xlsx"

data = pd.read_excel(xlsxfile,
                         sheet_name='one',
                         engine='openpyxl',
                         header=0,
                         dtype=str)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()


schema = StructType([ \
    StructField("date",DateType(),True), \
    StructField("pate",StringType(),True) \

  ])
spark_df= spark.createDataFrame(data)
spark_df.printSchema()
spark_df = spark_df.withColumn("modified",functions.Lit('ew'))
spark_df.show(10, False)
