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
import re
from StatusCodeError import StatusCodeError

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
            self.connectMysql()
            global mysqlInitialized
            global mysqlInitializeLock
            mysqlInitializeLock.acquire()
            if not mysqlInitialized:
                self.initializeMysql()
                mysqlInitialized=True
            mysqlInitializeLock.release()
            self.makeDir("D:/gfycat","下载根目录")
            while not self.url_queue.empty():
                url = self.url_queue.get()
                self.getListHtml(url)
                self.url_queue.task_done()

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
            print("%s数据库连接成功" % (self.threadName))
        except Exception as e:
            print("%s数据库连接异常，错误信息为%s" % (self.threadName, str(e)))

    def initializeMysql(self):
        try:
            with open("initialize.sql", 'r', encoding='utf-8') as fd:
                sqlStr = fd.read()
                sqlCommands = sqlStr.split(';')
                for command in sqlCommands:
                    if command != "":
                        self.mysqlClient.cursor().execute(command)
                        print("{}成功创建数据表{}".format(self.threadName, command.split("`")[1]))
                print('%s数据库初始化成功!' % (self.threadName))
        except BaseException as e:
            print("%s数据库初始化异常，错误信息为%s" % (self.threadName, str(e)))

    def getListHtml(self,url,repeat_count=0):
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(url, proxies=proxy,timeout=5)
            if response.status_code == 200:
                self.validip_que.put(validip)
                response.encoding = "utf-8"
                imgurls=re.findall(r'"file":{"bucket":"hbimg", "key":"([\S\s\\s\r\n]{0,}?)",', response.text, re.I)
                for imgurl in imgurls:
                    src="http://img.hb.aicdn.com/"+imgurl
                    self.sqlInsertFailedUrl(url, "image")
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("列表页下载异常，错误信息为%s" % (str(e)))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s列表页下载失败，正在进行第%d次重新下载!" % (url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("%s列表页下载失败，添加至数据库" % (url))
                self.sqlInsertFailedUrl(url,"list")

    def makeDir(self,dir_path,type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("成功创建%s%s" % (type,dir_path))
        except BaseException as e:
            print("创建%s异常，错误信息为%s"%(type,str(e)))

    def sqlInsertFailedUrl(self,url,type):
        try:
            global sql
            sql = """INSERT IGNORE INTO gfycat_failed_{}_url(url) VALUES ('{}')""".format(type,url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("成功插入一条错误的{}记录到数据库".format(type))
        except BaseException as e:
            print("sqlInsertFailedUrl抛出异常，异常内容为:{}".format(str(e)))

    def initializeImageRequestHeaders(self):
        with open('image_headers.txt', 'r') as f:
            headerStr = f.read()
            headersArr = headerStr.split('\n')
        self.image_headers = {}
        for headerItem in headersArr:
            headersItemName = headerItem.split(': ')[0]
            headerItemValue = headerItem.split(': ')[1] if headersItemName != 'User-Agent' else "%s" % (self.userAgents[self.count % 17])
            self.image_headers[headersItemName] = headerItemValue

def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(10000)
    ipCheckoutThreadMount = 20
    ipCollectThreadMount = 3
    dataCollectThreadMount =40
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(20)
    url_list=[]
    url_que = Queue(80000)
    for url in url_list:
        url_que.put(url)
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程%d" % (i), url_que,validip_que)
        worker.start()
        print("数据采集线程%d开启" % (i))
    url_que.join()

if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
