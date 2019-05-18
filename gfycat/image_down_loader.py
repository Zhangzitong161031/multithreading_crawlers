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
import random
import re
import warnings

class Spider(threading.Thread):
    def __init__(self, threadName, url_queue,validip_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName=threadName
        self.validip_que=validip_que
        self.mysqlConfig = MysqlConfig
        self.url_queue = url_queue
        self.userAgents =userAgents
        self.productPageRequestCount=0
        self.count = 0

    def run(self):
            print("%s开始启动" % (self.name))
            # self.connectMysql()
            # global mysqlInitialized
            # global mysqlInitializeLock
            # mysqlInitializeLock.acquire()
            # if not mysqlInitialized:
            #     self.initializeMysql()
            #     mysqlInitialized=True
            # mysqlInitializeLock.release()
            self.makeRootDir()
            while not self.url_queue.empty():
                url = self.url_queue.get()
                self.dowmloadImage(url)
                self.url_queue.task_done()


    def makeRootDir(self):
        try:
            if not os.path.exists("C:/gfycat"):
                os.mkdir("C:/gfycat")
                print("成功创建文件夹C:/gfycat")
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
            print("数据库连接成功")
        except Exception as e:
            print("数据库连接失败")

    def initializeMysql(self):
        with open("initialize.sql", 'r', encoding='utf-8') as fd:
            sqlStr=fd.read()
            sqlCommands = sqlStr.split(';')
            for command in sqlCommands:
                if command!="":
                    try:
                        self.mysqlClient.cursor().execute(command)
                        print("成功创建数据表" + command.split("`")[1])
                    except Exception as msg:
                        pass
                        # print(msg)
            print('数据库初始化成功!')

    def dowmloadImage(self,src,repeat_count=0):
        try:
            start_time=time.time()
            self.count+=1
            # self.initializeImageRequestHeaders()
            validip = self.validip_que.get()
            proxy = {'http': validip}
            img_response = requests.get(src, proxies=proxy)
            if img_response.status_code == 200 or img_response.status_code == 304:
                self.validip_que.put(validip)
                fileName = src.split("/")[-1]
                file_path = "C:/gfycat/%s" % (fileName)
                img_content = img_response.content
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName,file_path,end_time-start_time))
                    #self.sqlInsertCompleteImageUrl(src)
            else:
                self.validip_que.put(validip)
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

    def sqlInsertCompleteImageUrl(self, url):
        try:
            global sql
            sql = """INSERT IGNORE INTO gfycat_complete_imageurl(url) VALUES ('{}')""".format(url)
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
    ipCheckoutThreadMount = 30
    ipCollectThreadMount = 3
    dataCollectThreadMount =100
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(40)
    url_que = Queue(80000)
    with open('urls.txt', 'r') as f:
        urlsStr = f.read()
        urlsArr = urlsStr.split('\n')
    for url in urlsArr:
        url_que.put(url)
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程%d" % (i), url_que,validip_que)
        worker.start()
    url_que.join()

if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
