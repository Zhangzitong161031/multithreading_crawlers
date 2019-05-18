import os
import shutil

def format(path):
    if os.path.isdir(path):
        second_level_file_names=os.listdir(path)
        for second_level_file_name in second_level_file_names:
            second_level_file_path="%s/%s"%(path,second_level_file_name)
            if os.path.isdir(second_level_file_path):
                third_level_file_names=os.listdir(second_level_file_path)
                for third_level_file_name in third_level_file_names:
                    third_level_file_path="%s/%s"%(second_level_file_path,third_level_file_name)
                    if os.path.isdir(third_level_file_path):
                        new_dir_path="%s_%s" % (second_level_file_path, third_level_file_name)
                        if not os.path.exists(new_dir_path):
                            shutil.copytree(third_level_file_path, new_dir_path)
                shutil.rmtree(second_level_file_path)
                print("success processed%s"%(second_level_file_path))

rootDirArr=os.listdir(".")
for dir in rootDirArr:
    format(dir)
    format(dir)
    format(dir)