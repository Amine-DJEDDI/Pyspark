from pyspark.sql import *
from pyspark.sql.functions import lpad
from pyspark.sql.functions import col

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


xlsxfile = "/home/amine/Desktop/siren_leie.xlsx"

data = pd.read_excel(xlsxfile,
                         sheet_name='one',
                         engine='openpyxl',
                         header=0,
                         dtype=str)

spark_df= spark.createDataFrame(data)
spark_df.show(10, False)

spark_df = spark_df.withColumn('siren',  lpad(spark_df.siren, 9, '0'))

spark_df = spark_df.withColumn("contains_cat", col("siren").rlike("^[0-9]{9}$"))


spark_df = spark_df.withColumn("siren match", col("siren").rlike("^[0-9]{9}$"))
spark_df = spark_df.withColumn("lei match", col("lei").rlike("[A-Za-z]{1}"))


ww = spark.sql("select * from spark_df")


df = spark_df.filter(spark_df["siren"].rlike("(?![0-9]{9}$)")).count()
df2 = spark_df.filter(spark_df["lei"].rlike("[a-zA-Z][0-9]{1}$")).count()
print(df)
print(df2)


#spark_df = spark.sql("select * from spark_df where rlike(alphanumeric,'^[0-9]')")
#spark.sql("SELECT salary*100 as salary, salary*-1 as CopiedColumn, 'USA' as country FROM PERSON").show()

#spark_df= spark.sql("SELECT salary*100 as salary")




spark_df.show(10, False)
