import pandas as pd
import os
import re
import sys
import os
import glob
import time
path = "/home/amine/Desktop/enlineremove.xlsx"

df = pd.read_excel(path)
print(df)

df_column_list = []


for element in df:
    df_column_list.append(element)

s= [re.sub(r'[^a-zA-Z0-9]', '', i) for i in df_column_list ]

print(s)
print('\n')

print('\n')
df.columns = s
print(df)
