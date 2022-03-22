from pyspark.sql import *
import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("inner join").getOrCreate()

df = spark.read.format("csv").option("delimiter",";").option("header", "true").load("table1_1.csv")
df2 = spark.read.format("csv").option("delimiter",";").option("header", "true").load("table1_2.csv")


full_joy = df.alias('a').join(df2.alias('b'), (F.col('a.banque') == F.col('b.banque'))& \
                              (F.col('a.tiers') == F.col('b.tiers')) , 'full')

full_joy.persist()
full_joy.show()


all_column = full_joy.schema.names
all_index = []
print(all_column)

a= (len(all_column))
print(a)


lst = []
for i in range(a):
    lst.append(i)
print(lst)

aaa = zip(all_column, lst)
aaa = list(aaa)
print(aaa)





full_joy.show()








'''
df = df.withColumnRenamed('banque', 'xx_banque')
df = df.withColumnRenamed('tiers', 'xx_tiers')
full_joy2 = df2.join(df, (df2.banque == df.xx_banque)& (df2.tiers == df.xx_tiers) , 'fullouter').drop('xx_tiers', 'xx_banque')
full_joy2.show()
'''




