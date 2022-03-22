from pyspark.sql import *
import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("inner join").getOrCreate()

df = spark.read.format("csv").option("delimiter",";").option("header", "true").load("table1.csv")

inner_df = df.alias('emp1').join(df.alias('emp2') , F.col('emp1.staff_id') == F.col('emp2.manager_id') , 'inner')
inner_df.show()
