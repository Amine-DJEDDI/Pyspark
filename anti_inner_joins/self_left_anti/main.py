from pyspark.sql import *
import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("inner join").getOrCreate()

df = spark.read.format("csv").option("delimiter",";").option("header", "true").load("table1.csv")
inner_df = df.alias('emp1').join(df.alias('emp2') , ['staff_id', 'manager_id', 'first_name', 'last_name'], 'leftanti')

'''
to get only managers
+--------+----------+----------+---------+
|staff_id|manager_id|first_name|last_name|
+--------+----------+----------+---------+
|       1|      null|   Fabiola|  Jackson|
+--------+----------+----------+---------+
'''
inner_df.show()

'''
'inner', 
'outer', 
'full', 
'fullouter', 
'full_outer', 
'leftouter', 
'left',
'left_outer',
'rightouter',
'right',
'right_outer',
'leftsemi',
'left_semi',
'semi',
'leftanti',
'left_anti',
'anti', 
cross'
'''
