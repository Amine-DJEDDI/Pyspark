import pyspark.sql

from commun.sparkSession import *
# in this example spark warehouse will be used instead of hive warehouse
from tries.functions import *

#nom = 'aaaa'
#create_database(nom)


spark.sql("show databases").show()
list_db = db_in_spark_warehouse(spark_warehouse_path)
drop(list_db)

spark.sql("show databases").show()

