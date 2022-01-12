from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F


import pandas as pd
from pyspark.sql.types import *
# warehouse_location points to the default location for managed databases and tables
from pyspark.sql.functions import *


xlsxfile = "/home/amine/Desktop/siren_lpad.xlsx"

spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").getOrCreate()
data = pd.read_excel(xlsxfile,sheet_name='one',engine='openpyxl',header=0,dtype=str)
schema = StructType([StructField("siren",StringType(),True),StructField("nom",StringType(),True),])
spark_df= spark.createDataFrame(data = data , schema = schema)

spark_df.printSchema()

spark_df.show()
#spark_df = spark_df.withColumn('siren', F.when(F.length(F.col('siren'))<9 ,F.expr('lpad(siren, 9, 0)')).otherwise(F.col('siren')))



spark_df = spark_df.withColumn('siren', F.expr("CASE WHEN length(siren) < 9 THEN lpad(siren, 9, 0) ELSE siren END"))

spark_df.show()

