from pyspark.sql import *
import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("inner join").getOrCreate()

df = spark.read.format("csv").option("delimiter",";").option("header", "true").load("table1.csv")

inner_df = df.alias('emp1').join(df.alias('emp2') , F.col('emp1.staff_id') == F.col('emp2.manager_id') , 'inner')

''''
+--------+----------+---------+----------+--------+----------+---------+----------+
|staff_id|first_name|last_name|manager_id|staff_id|first_name|last_name|manager_id|
+--------+----------+---------+----------+--------+----------+---------+----------+
|       1|   Fabiola|  Jackson|      null|       2|   Mariola|    anchi|         1|
|       2|   Mariola|    anchi|         1|       4|      Rami|     jaki|         2|
|       2|   Mariola|    anchi|         1|       3|   Saviola|    Barti|         2|
|       3|   Saviola|    Barti|         2|       6|     Layla|   Terrel|         3|
|       3|   Saviola|    Barti|         2|       5|    Venita|   Daneil|         3|
|       4|      Rami|     jaki|         2|       9|  Nourdine|    Benor|         4|
|       4|      Rami|     jaki|         2|       8|     Genna|     Beni|         4|
|       4|      Rami|     jaki|         2|       7|   Janette|   Mackso|         4|
+--------+----------+---------+----------+--------+----------+---------+----------+
'''

inner_df = df.alias('emp1').join(df.alias('emp2') , ['staff_id','manager_id','first_name','last_name'] , 'inner')
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
+--------+----------+----------+---------+
'''


inner_df.show()
