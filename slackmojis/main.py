import requests
import threading
from queue import Queue
import time
from bs4 import BeautifulSoup
import os
import pymysql
from mysqlConfig import MysqlConfig
import url_list
from StatusCodeError import StatusCodeError

class Spider(threading.Thread):
    def __init__(self, threadName, url_queue, validip_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.mysqlConfig = MysqlConfig
        self.url_queue = url_queue
        self.validip_que = validip_que
        self.threadName=threadName

    def run(self):
        print("%s开始启动" % (self.name))
        self.connectMysql()
        self.initializeMysql()
        if not os.path.exists("D:/slackmojis"):
            os.mkdir("D:/slackmojis")
            print("成功创建文件夹D:/slackmojis")
        while not self.url_queue.empty():
            url=self.url_queue.get()
            self.getHtml(url)

    def getHtml(self,url,repeat_count=0):
        try:
            response = requests.get(url,timeout=8)
            if response.status_code == 200:
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                img_list = soup.select(".downloader img")
                for img in img_list:
                    src=img.get("src")
                    file_name=src.split("/")[-1].split("?")[0]
                    file_path="D:/slackmojis/%s"%(file_name)
                    self.downloadImage(src,file_path)
        except:
            print("页面请求出错")


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
            print("%s数据库连接成功"%(self.threadName))
        except Exception as e:
            print("%s数据库连接异常，错误信息为%s"%(self.threadName,str(e)))

    def initializeMysql(self):
        try:
            with open("initialize.sql",'r',encoding='utf-8') as fd:
                sqlStr=fd.read()
                sqlCommands = sqlStr.split(';')
                for command in sqlCommands:
                    if command!="":
                        self.mysqlClient.cursor().execute(command)
                        print("{}成功创建数据表{}".format(self.threadName,command.split("`")[1]))
                print('%s数据库初始化成功!'%(self.threadName))
        except BaseException as e:
            print("%s数据库初始化异常，错误信息为%s"%(self.threadName,str(e)))

    def downloadImage(self,src,file_path,repeat_count=0):
        try:
            start_time=time.time()
            response = requests.get(src)
            if response.status_code==200:
                img_content=response.text
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    inter = end_time - start_time
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName,file_path, inter))
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("%s图片下载异常，错误信息为%s" % (self.threadName, str(e)))
            repeat_count += 1
            if repeat_count < 4:
                print("%s图片%s下载抛出异常，正在进行第%d次重新下载!" % (self.threadName,src, repeat_count))
                self.downloadImage(src, file_path, repeat_count)
            else:
                print("%s图片%s下载失败，将添加下载失败信息到数据表" % (self.threadName,src))
                self.sqlInsertFailedUrl(src,"image")

    def sqlInsertFailedUrl(self,url,type):
        try:
            global sql
            sql = """INSERT IGNORE INTO slackmojis_failed_{}_url(url) VALUES ('{}')""".format(type,url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("%s成功插入一条错误的{}记录到数据库".format(self.threadName,type))
        except BaseException as e:
            print("%s的sqlInsertFailedUrl抛出异常，异常内容为:{}".format(self.threadName,str(e)))


def main(url_list):
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(100000)
    ipCheckoutThreadMount = 35
    ipCollectThreadMount = 2
    dataCollectThreadMount = 100
    url_list = url_list
    url_que = Queue(5000)
    for arc_url in url_list:
        url_que.put(arc_url)
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程%d" % (i), url_que, validip_que)
        print("数据采集线程%d开启" % (i))
        worker.start()

    url_que.join()

if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    url_list = url_list.url_list
    main(url_list)
