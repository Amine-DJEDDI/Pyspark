from pyspark.sql import *
from pyspark.sql.functions import lpad
from pyspark.sql.functions import col

from pyspark.sql.functions import *
import pyspark.sql.functions as F
from os.path import abspath
from pyspark.sql import Row
import pandas as pd
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
# warehouse_location points to the default location for managed databases and tables
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

xlsxfile = "/home/amine/Desktop/siren_leie.xlsx"
spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").getOrCreate()
data = pd.read_excel(xlsxfile,sheet_name='one',engine='openpyxl',header=0,dtype=str)
schema = StructType([StructField("siren",StringType(),True),StructField("lei",StringType(),True),])
spark_df= spark.createDataFrame(data = data , schema = schema)

columns = spark_df.columns
for column in columns:
    spark_df = spark_df.withColumn(column,F.when(F.isnan(F.col(column)),None).otherwise(F.col(column)))

#fill siren column with zerros !
spark_df = spark_df.withColumn('siren',  lpad(spark_df.siren, 9, '0'))
#check if siren contains only decimals and has a length of 9 decimals (need the negation of regex)
spark_df = spark_df.withColumn("siren_contains_decimals", col("siren").rlike("^(?![0-9]+$)"))
# counts only rows matching regex
df = spark_df.filter(spark_df["siren"].rlike("^(?![0-9]+$)")).count() # count that conatins alphanumeric





spark_df = spark_df.withColumn("lei_length", col("lei").rlike("^(?![0-9a-zA-Z]{20}$)"))
not_good= spark_df.filter(length(col("lei")) != 20).count()
spark_df.show(10, False)











#spark_df =  spark_df.withColumn("siren",when(col("siren").isin(None), lit('0')).otherwise(col("siren")))
#spark_df =  spark_df.withColumn("siren",when(spark_df.siren.isNull(), "0").otherwise(spark_df.siren))
#fd = spark_df.filter(spark_df["siren"].rlike("^[0-9]+$")).count()
#spark_df = spark_df.withColumn("siren_contains_decimals", col("siren").rlike("^(?![0-9]+$)"))
'''
def replace(column, value):
    return when(column != value, column).otherwise(lit(None))
spark_df = spark_df.withColumn("siren", replace(col("siren"), "000000000")).show()
'''