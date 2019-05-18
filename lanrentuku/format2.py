import os
import shutil

formatBaseDirArr=["youmeitu"]
def format(path,level=3):
    second_name_list=os.listdir(path)
    for second_dir_name in second_name_list:
        second_dir_path="%s/%s"%(path,second_dir_name)
        third_dir_list=os.listdir(second_dir_path)
        for third_dir_name in third_dir_list:
            third_dir_path="%s/%s"%(second_dir_path,third_dir_name)
            if os.path.isdir(third_dir_path):
                shutil.copytree(third_dir_path,"%s_%s"%(second_dir_path,third_dir_name))
        shutil.rmtree(second_dir_path)
        print("success processed a category %s"%(second_dir_name))

for baseDir in formatBaseDirArr:
    format(baseDir)