import json
import os
import pandas as pd
pd.set_option('display.max_columns', None)

xlsxfile = "/home/amine/PycharmProjects/hive/hanane/rsvero_parcs_vp_2020_france.xlsx"

data = pd.read_excel(xlsxfile)
print(data)

