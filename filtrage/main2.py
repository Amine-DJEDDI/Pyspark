import pandas as pd
import os
import re
import json
import sys
import os
import glob
import time
path = "/home/amine/Desktop/enlineremove.xlsx"

df = pd.read_excel(path)
print(df)
df = df.rename(columns=lambda x: re.sub('\n','',x))
df = df.rename(columns=lambda x: re.sub(r'[^a-zA-Z0-9]','',x))

print('\n')
# indexNames = global_data[(global_data['numero_de_siren_si_disponible'] == '000000000') & (global_data['numero_de_lei_si_disponible'] == '00000000000000000000')].index
# global_data.drop(indexNames , inplace=True)
print(df)




