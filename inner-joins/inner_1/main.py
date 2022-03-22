from pyspark.sql import *
import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("inner join").getOrCreate()

df = spark.read.format("csv").option("delimiter",";").option("header", "true").load("inner-joins/inner_1/table1.csv")
df2 = spark.read.format("csv").option("delimiter",";").option("header", "true").load("inner-joins/inner_1/table2.csv")
df.show()
df2.show()
inner_df = df.join(df2 , ['id'], 'inner')
inner_df.show()