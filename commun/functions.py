from tries.settings import *

#create databse in spark warehouse :
def create_database (db_name):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

def create_table (table_name):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name}")



#show all databases in the spark-warehouse :
def db_in_spark_warehouse (spark_warehouse_path) :
    list_of_db = []
    for file in os.listdir(spark_warehouse_path):
        file = file.split('.')[0]
        list_of_db.append(file)
    return list_of_db


#drop all databses in the spark warehouse
def drop(list_db_spark_warehouse):
    for db in list_db_spark_warehouse :
        spark.sql(f"DROP DATABASE {db}")