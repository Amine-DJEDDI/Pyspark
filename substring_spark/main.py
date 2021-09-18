from pyspark.sql import *

from pyspark.sql.functions import *
import pyspark.sql.functions as F
from os.path import abspath
from pyspark.sql import Row
import pandas as pd

# warehouse_location points to the default location for managed databases and tables
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .getOrCreate()


xlsxfile = "/home/amine/Desktop/substring.xlsx"

data = pd.read_excel(xlsxfile,
                         sheet_name='one',
                         engine='openpyxl',
                         header=0,
                         dtype=str)

spark_df= spark.createDataFrame(data)

aa = '20210722_donnees_complementaires_dphr.xlsx'
#solution manuel marche same
#spark_df = spark_df.withColumn('xx_filename', F.col('xx_filename').substr(10,33) )
#spark_df = spark_df.withColumn('xx_filename', F.substring('xx_filename', 10, 33))





#spark_df = spark_df.withColumn('length', length(spark_df.xx_filename))
#spark_df = spark_df.withColumn('xx_filename', F.substring('xx_filename', 10, ))


udf_slicer = udf(lambda x:x[9:-1],StringType())
spark_df = spark_df.withColumn('xx_filename',udf_slicer('xx_filename'))




spark_df.show(10 , False)


