import pandas as pd
import json
import re

pathmeta = "/home/amine/PycharmProjects/hive/jsonfiles/meta.json"

# filter and split the main json based on regex
json_meta_data = json.load(open(pathmeta))['excels_tables'] # return a list

regex = "^collect"
def desired_meta ():
    parm_meta_data = []
    for i in json_meta_data:
        hive_table = i['hive_table']
        matched = re.match(regex, hive_table)
        if matched:
            parm_meta_data.append(i)

    with open('meta_file_name', 'w') as outfile:
        json.dump(parm_meta_data, outfile, indent=4)


desired_meta()













#test_string = 'a1b2cdefg'
#matched = re.match("[a-z][0-9][a-z][0-9]+", test_string)
#is_match = bool(matched)
#print(is_match)
#output_dict = [x for x in json_meta_data if x['hive_table'] == 'collecte_sfil_concentration_nominale_detail']

