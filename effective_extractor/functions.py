from sqlalchemy import false

from effective_extractor.settings import *


def extractor (path_common_csvs, path_meta_data):

    json_meta_data = json.load(open(path_meta_data))['excels_tables']  # return array of objects of meta data related to excel tables
    # print(json_meta_data[0]['excel_filename'])
    for object in json_meta_data:

        header =             object['excel_header'] - 1
        excel_path =     object['excel_path']
        sheet_name =         object['excel_sheet_name']
        usecols =            object['excel_usecols']
        nrows =              object['excel_nrows']

        full_excel_path = path_common_csvs + excel_path
        subdirectory = excel_path.split('/')[0]


        list_excel_files = glob.glob(full_excel_path)

        print('\n')
        print('============================================== '+excel_path + '=============================')
        print(list_excel_files)

        global_data = pd.DataFrame()
        for excel_file in list_excel_files:


            data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', header=header,
                             usecols=usecols, nrows=nrows, dtype=str )
            data['xx_filename'] = excel_file.split('/')[-1]
            print(data)
            print('\n')

            global_data = global_data.append(data, ignore_index=True)


        print('========================global_dataframe ===========================================================')
        print(global_data)
        print('===================SPARK PART========================================================================')
        spark_df= spark.createDataFrame(global_data, schema= StructType([ StructField(pd_col,StringType(),True) for pd_col in global_data.columns.values ]))

        spark_df = spark_df.withColumn('xx_input_path', F.concat(F.lit(ingest_excel_input_path), F.lit(subdirectory)).cast('string'))
        spark_df = spark_df.withColumn('xx_archive_path', F.lit("{0}{1}/{2}/".format(ingest_excel_archive_path, date_ctrlm, date_ingestion) + subdirectory).cast('string'))
        spark_df = spark_df.withColumn('xx_archive_path1', F.concat(F.lit(ingest_excel_archive_path), F.lit(date_ctrlm) , F.lit('/') ,F.lit(date_ingestion), F.lit('/'), F.lit(subdirectory)).cast('string'))

        spark_df.show(10, False)
        print('==============================''FIN''================================================================')


