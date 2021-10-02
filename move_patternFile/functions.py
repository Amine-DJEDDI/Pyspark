import subprocess

from move_patternFile.settings import *




def copy_Files_to_targed_Folder (src_path, trgt_path) :
    file_list = glob.glob(src_path+"/*.xlsx")
    for file in file_list :
        print(file)
        subprocess.run(f"mv {file} {trgt_path}",
                                                    stdout=subprocess.PIPE,
                                                    stderr=subprocess.PIPE,
                                                    universal_newlines=True,
                                                    shell= True)






