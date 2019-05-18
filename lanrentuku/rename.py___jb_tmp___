import os
import datetime
import random

baseDirArr=["jiuwa","huiyi8"]
for baseDir in baseDirArr:
    dirArr=os.listdir(baseDir)
    for dirName in dirArr:
        time = datetime.datetime.now().microsecond
        newName="{}/{}{}".format(baseDir,time,random.randint(10000,99999))
        oldName="{}/{}".format(baseDir,dirName)
        os.rename(oldName,newName)




