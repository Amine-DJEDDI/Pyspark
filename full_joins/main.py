from pyspark.sql import *
import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("inner join").getOrCreate()

df = spark.read.format("csv").option("delimiter",";").option("header", "true").load("table1.csv")
df2 = spark.read.format("csv").option("delimiter",";").option("header", "true").load("table2.csv")
df.show()
df2.show()

full_joy = df.join(df2, df2.id == df.id , 'full')
full_joy.show()

full_joy2 = df2.join(df, df2.id == df.id , 'fullouter')
full_joy2.show()