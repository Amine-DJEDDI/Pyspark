from pyspark.sql import *
import pyspark.sql.functions as F
from os.path import abspath
from pyspark.sql import Row
import pandas as pd
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

from pyspark.sql.types import StringType

xlsxfile = "/home/amine/Desktop/accesrow.xlsx"
spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").getOrCreate()
data = pd.read_excel(xlsxfile,sheet_name='one',engine='openpyxl',header=0,dtype=str)
schema = StructType([StructField("num_lei",StringType(),True),StructField("filepath",StringType(),True),])
spark_df= spark.createDataFrame(data = data , schema = schema)


dd= spark_df.filter(F.expr('num_lei rlike "^(?![0-9a-zA-Z]{20}$)"')).count()
print(f'dd = {dd}')

dww = spark_df.filter(F.expr('num_lei rlike "^(?![0-9a-zA-Z]{20}$)"')).select(spark_df.filepath)
list= spark_df.filter(F.expr('num_lei rlike "^(?![0-9a-zA-Z]{20}$)"')).select(spark_df.filepath).collect()

spark_df.show(10, False)
dww.show(10,False)
print(list)
listaw = [ row.filepath for row in list]
print(listaw)




