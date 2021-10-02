from move_patternFile_to_nonExistingFile.settings import *

def copy_Files_to_targed_Folder (src_path, root_trgt_path ,folder_to_create) :
    file_list = glob.glob(src_path+"/*.xlsx")

    '''check if the zone_b folder existe , if not create it '''
    targer_file = root_trgt_path + "/" + folder_to_create
    if not os.path.exists(targer_file):
        os.makedirs(targer_file)

    if os.path.exists(targer_file):
        filelist = [f for f in os.listdir(targer_file) ]
        for f in filelist:
            os.remove(os.path.join(targer_file, f))


    for file in file_list :
        subprocess.run(f"cp {file} {targer_file}",
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       universal_newlines=True,
                       shell=True)












