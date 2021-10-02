this code is about to :
- copy a bunch of files from folder (zone_a) to another folder named (zone_b)
- the target folder (zone_b) already exists



'''
for displaying all pattern recursivly : use of /**/
recursivly means : all files of the directory and the sub directory that match the pattern
file_list = glob.glob(src_path+"/**/"+"/*.xlsx", recursive=True)
this commande show the files in src_path and also files  of subdirectory of src_path
'''