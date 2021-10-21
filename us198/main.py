from pyspark.sql import *
import pyspark.sql.functions as F
from os.path import abspath
from pyspark.sql import Row
import pandas as pd
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

from pyspark.sql.types import StringType

xlsxfile = "/home/amine/Desktop/accescolsee.xlsx"
spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").getOrCreate()
data = pd.read_excel(xlsxfile,sheet_name='one',engine='openpyxl',header=0,dtype=str)
schema = StructType([StructField("num_lei1",StringType(),True),
                     StructField("num_lei",StringType(),True),
                    StructField("siren",StringType(),True)
                     ])


spark_df= spark.createDataFrame(data = data , schema = schema)

columns = spark_df.columns
for column in columns:
    spark_df = spark_df.withColumn(column,F.when(F.isnan(F.col(column)),None).otherwise(F.col(column)))

num_lei1 = spark_df['num_lei1']
num_lei = spark_df['num_lei']
new_df = spark_df.withColumn('coalsce_lei', F.coalesce(num_lei1 , num_lei))

new_df.show(10, False)

print('================')
spark_df.show(10, False)