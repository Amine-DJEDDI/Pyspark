
import time
import datetime
import re
import unicodedata
import pandas as pd
import numpy as np
import logging
import os, shutil
#import xlrd
import json
import glob
import sys
#from datetime import datetime
from datetime import date
from pyspark.sql import *
import pyspark.sql.functions as F
from os.path import abspath
from pyspark.sql import Row
import pandas as pd
from pyspark.sql.types import *

dest_table = {"table_name": "tiers_caract_base", "payload":
    [
        {"col_name": "reporting_date", "col_type": "date"}
        , {"col_name": "sequence_flux", "col_type": "bigint"}
        , {"col_name": "code_etablissement", "col_type": "string"}
    ]
            }

print(dest_table["payload"])

def getColumnList(crudeSchema):
    columnList=[]
    for colMeta in crudeSchema:
        columnList.append(colMeta["col_name"].lower())
    return columnList

schema_dest_table=getColumnList(dest_table["payload"])

print(schema_dest_table)

t_start=int(time.time())
print(t_start)