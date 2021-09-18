
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

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .getOrCreate()

data = [("0","FF1"),("1","GG2"),
        (None,"FFFFGGGGHHHHYYYYUUUU"),("123456789",None),
        ("01F2","FGT3")]

columns = ["siren","lei"]
df = spark.createDataFrame(data = data, schema = columns)
df = df.withColumn('siren',  lpad(df.siren, 9, '0')) #fill with zeros , if null ignored
#df =  df.withColumn("siren",when(df.siren.isNull(), "0").otherwise(df.siren)) #do the work

df = df.withColumn("siren_contains_decimals", col("siren").rlike("^(?![0-9]+$)"))

df.show(10, False)