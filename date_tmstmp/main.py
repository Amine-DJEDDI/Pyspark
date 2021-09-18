from pyspark.sql import *
from os.path import abspath
from pyspark.sql import Row

# warehouse_location points to the default location for managed databases and tables
from pyspark.sql.functions import *

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()




# spark is an existing SparkSession
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("CREATE TABLE IF NOT EXISTS tttt (key INT, value STRING) USING hive")
ed = spark.sql("select * from tttt")
ed.withColumn('ee', lit("1")).show()
ed.write.mode("overwrite").saveAsTable("test_db.test_table2")