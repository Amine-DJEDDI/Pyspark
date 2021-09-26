from pyspark.sql import *
from os.path import abspath
from pyspark.sql import Row
import logging
import os
import pandas as pd


# warehouse_location points to the default location for managed databases and tables
from pyspark.sql.functions import *

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", "/home/amine/PycharmProjects/hive/commun/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()
