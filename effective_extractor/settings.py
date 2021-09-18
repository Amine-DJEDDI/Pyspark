#import :

import pandas as pd
import os
import  re
import json
import sys
import os
import glob
import time

import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()





#Paths :
path_common_csvs = "/home/amine/Desktop/csvs/"
path_meta_data = "/home/amine/PycharmProjects/pyspark/effective_extractor/meta_data.json"

ingest_excel_input_path = "env/ep/flux_entrant/ri2/depot/excel_input/"


date_ctrlm = '20201112'
ingest_excel_archive_path=  'env/ep/flux_entrant/ri2/archive/ingest_excel/'
date_ingestion = time.strftime("%Y%m%d%H%M%S", time.gmtime())