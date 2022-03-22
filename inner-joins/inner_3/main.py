from pyspark.sql import *
import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("inner join").getOrCreate()

df = spark.read.format("csv").option("delimiter",";").option("header", "true").load("table1.csv")

inner_df = df.alias('emp1').join(df.alias('emp2') , ['staff_id', 'manager_id', 'first_name', 'last_name'], 'inner')
df_tmp = df.selectExpr('staff_id as  staff_id_tmp','first_name as manager_first', 'last_name as manager_last_name')

df_tmp.printSchema()

last_df = inner_df.join(df_tmp ,inner_df.manager_id == df_tmp.staff_id_tmp , 'inner')
listcolumns = ['staff_id','first_name', 'last_name', 'manager_id','manager_first', 'manager_last_name' ]
last_df = last_df.select(listcolumns)


'''
+--------+----------+----------+---------+
|staff_id|manager_id|first_name|last_name|
+--------+----------+----------+---------+
|       2|         1|   Mariola|    anchi|
|       3|         2|   Saviola|    Barti|
|       4|         2|      Rami|     jaki|
|       5|         3|    Venita|   Daneil|
|       6|         3|     Layla|   Terrel|
|       7|         4|   Janette|   Mackso|
|       8|         4|     Genna|     Beni|
|       9|         4|  Nourdine|    Benor|
+--------+----------+----------+---------+'''

last_df.show()
