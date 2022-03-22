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
inner_df = df2.join(df , ['id'], 'leftanti')
#retourne les elements de df2 qui ne sont pas dans df
#means construct a dataframe with elements in df2 THAT ARE NOT in df.
inner_df2 = df.join(df2 , ['id'], 'leftanti')
##retourne les elements de df qui ne sont pas dans df2

final = inner_df.union(inner_df2)
#return les elements qui ne  match pas

'''
two leftanti in two direction and union the result :
give you the non matching rows
+---+------+--------+
| id| price|location|
+---+------+--------+
| 10|    66|  panada|
|  5|damine|   deddi|
+---+------+--------+
'''
final.show()
