# -*- coding:utf-8 -*-
import requests
import threading
from queue import Queue
import time
from proxy_helper import Proxy_helper
from userAgents import userAgents
import os
import pymysql
from mysqlConfig import MysqlConfig
from image_infos_collecter import ImageInfoCollecter


class Spider(threading.Thread):
    def __init__(self, threadName, imageInfo_que,validip_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName=threadName
        self.validip_que=validip_que
        self.mysqlConfig = MysqlConfig
        self.imageInfo_que = imageInfo_que
        self.userAgents =userAgents
        self.count=0

    def run(self):
            print("%s开始启动" % (self.name))
            self.connectMysql()
            global mysqlInitialized
            global mysqlInitializeLock
            mysqlInitializeLock.acquire()
            if not mysqlInitialized:
                self.initializeMysql()
                mysqlInitialized=True
            mysqlInitializeLock.release()
            self.makeRootDir()
            self.start_work()

    def start_work(self):
        while not self.imageInfo_que.empty():
            imageInfo = self.imageInfo_que.get()
            self.dowmloadImage(imageInfo)
            self.imageInfo_que.task_done()
        else:
            print("队列里图片数量为空，等待3秒!")
            time.sleep(5)
            self.start_work()
    def makeRootDir(self):
        try:
            if not os.path.exists("E:/huaban"):
                os.mkdir("E:/huaban")
                print("成功创建文件夹E:/huaban")
        except BaseException as e:
            print("makeRootDir抛出异常")
            print(e)

    def initializeImageRequestHeaders(self):
        with open('image_headers.txt', 'r') as f:
            headerStr = f.read()
            headersArr = headerStr.split('\n')
        self.image_headers = {}
        for headerItem in headersArr:
            headersItemName = headerItem.split(': ')[0]
            headerItemValue = headerItem.split(': ')[1] if headersItemName != 'User-Agent' else "%s" % (self.userAgents[self.count % 17])
            self.image_headers[headersItemName] = headerItemValue

    def connectMysql(self):
        try:
            self.mysqlClient = pymysql.connect(
                host=self.mysqlConfig.host,
                port=self.mysqlConfig.port,
                user=self.mysqlConfig.user,
                passwd=self.mysqlConfig.password,
                database=self.mysqlConfig.database,
                use_unicode=True
            )
            print("%s数据库连接成功"%(self.name))
        except Exception as e:
            print("%s数据库连接失败"%(self.name))

    def initializeMysql(self):
        with open("initialize.sql", 'r', encoding='utf-8') as fd:
            sqlStr=fd.read()
            sqlCommands = sqlStr.split(';')
            for command in sqlCommands:
                if command!="":
                    try:
                        self.mysqlClient.cursor().execute(command)
                        print("%s成功创建数据表"%(self.name) + command.split("`")[1])
                    except Exception as msg:
                        pass
                        # print(msg)
            print('%s数据库初始化成功!'%(self.name))

    def dowmloadImage(self,imageInfo,repeat_count=0):
        src = imageInfo.split("&&&&&&")[0]
        alt = imageInfo.split("&&&&&&")[1]
        try:
            start_time=time.time()
            self.count+=1
            self.initializeImageRequestHeaders()
            img_response = requests.get(src,headers=self.image_headers)
            if img_response.status_code == 200 or img_response.status_code == 304:
                dir_name = time.strftime('%Y%m%d%H', time.localtime(time.time()))
                dir_path = "C:/bee-ji/%s" % (dir_name)
                file_extension = img_response.headers.get("Content-Type").replace("image/", "").replace("jpeg", "jpg")
                file_name="{}.{}".format(alt,file_extension)
                file_path = "{}/{}" .format(dir_path,file_name)
                img_content = img_response.content
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName,file_path,end_time-start_time))
                    self.sqlInsertCompleteImageUrl(imageInfo)
            else:
                repeat_count += 1
                if repeat_count < 4:
                    print("%s图片返回状态码为%d，正在发送第%d次请求!" % (src,img_response.status_code, repeat_count))
                    self.dowmloadImage(src, repeat_count)
                else:
                    print("%s图片下载失败" % (src))
        except BaseException as e:
            print("dowmloadImage函数抛出异常")
            print(e)
            repeat_count += 1
            if repeat_count < 4:
                print("%s图片下载失败，正在进行第%d次重新下载!" % (src, repeat_count))
                self.dowmloadImage(src, repeat_count)
            else:
                print("%s图片下载失败" % (src))

    def sqlInsertCompleteImageUrl(self, infos):
        try:
            global sql
            sql = """INSERT IGNORE INTO huaban_complete_image_infos(infos) VALUES ('{}')""".format(infos)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("成功插入一image条记录")
            else:
                print("记录已存在，插入失败!")
        except BaseException as e:
            print("数据插入失败")

def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(10000)
    imageInfo_que=Queue(700000)
    ipCheckoutThreadMount = 5
    ipCollectThreadMount = 1
    dataCollectThreadMount =3
    imageInfoCollecter=ImageInfoCollecter("图片src采集线程",imageInfo_que)
    imageInfoCollecter.start()
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(4)
    for i in range(dataCollectThreadMount):
        worker = Spider("图片下载线程%d" % (i), imageInfo_que,validip_que)
        worker.start()
        worker.join()

if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
