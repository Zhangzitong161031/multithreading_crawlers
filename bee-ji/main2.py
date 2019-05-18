# -*- coding:utf-8 -*-
import requests
import threading
from queue import Queue
import time
from bs4 import BeautifulSoup
import os
import pymysql
from proxy_helper import Proxy_helper
from mysqlConfig import MysqlConfig
from StatusCodeError import StatusCodeError
import random

class Spider(threading.Thread):
    def __init__(self, threadName, url_queue, validip_que):
        threading.Thread.__init__(self)
        self.name=threadName
        self.daemon = True
        self.mysqlConfig = MysqlConfig
        self.url_queue = url_queue
        self.validip_que = validip_que

    def run(self):
        print("%s开始启动" % (self.name))
        self.makeDir("C:/bee-ji/","下载根目录")
        self.connectMysql()
        global mysqlInitialized
        global mysqlInitializeLock
        mysqlInitializeLock.acquire()
        if not mysqlInitialized:
            self.initializeMysql()
            mysqlInitialized=True
        mysqlInitializeLock.release()
        while not self.url_queue.empty():
            url = self.url_queue.get()
            self.getArcHtml(url)
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
            print("%s数据库连接成功"%(self.name))
        except Exception as e:
            print("%s数据库连接异常，错误信息为%s"%(self.name,str(e)))

    def initializeMysql(self):
        try:
            with open("initialize.sql",'r',encoding='utf-8') as fd:
                sqlStr=fd.read()
                sqlCommands = sqlStr.split(';')
                for command in sqlCommands:
                    if command!="":
                        self.mysqlClient.cursor().execute(command)
                        print("{}成功创建数据表{}".format(self.name,command.split("`")[1]))
                print('%s数据库初始化成功!'%(self.name))
        except BaseException as e:
            print("%s数据库初始化异常，错误信息为%s"%(self.name,str(e)))

    def getArcHtml(self,url,repeat_count=0):
        start_time=time.time()
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(url, proxies=proxy,timeout=16)
            if response.status_code == 200:
                self.validip_que.put(validip)
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                img = soup.select('.jss5 img')[0]
                src=img.get("src")
                if (src != "http://image.bee-ji.com/undefined"):
                    alt = img.get("alt")
                    invalid_str_arr = ["/", ".", "\\", "\r\n", "。", "*", '"', "<", ">", "|", "?", "？", ":"]
                    for invalid_str in invalid_str_arr:
                        alt = alt.replace(invalid_str, "")
                    self.downloadImage(src,alt)
                else:
                    print("详情页图片失效%s"%(url))
                    self.sqlInsertFailedUrl(url, "article")
            else:
                raise StatusCodeError("%s状态码错误，返回状态码为%d" % (url, response.status_code))
        except BaseException as e:
            print("%s详情页请求异常，错误信息为%s" % (self.name,str(e)))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s表情详情页%s下载失败，正在进行第%d次重新下载!" % (self.name,url, repeat_count))
                self.getArcHtml(url, repeat_count)
            else:
                print("%s表情详情页%s下载失败" % (self.name,url))
                self.sqlInsertFailedUrl(url,"article")

    def downloadImage(self,url,alt,repeat_count=0):
        start_time = time.time()
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(url, proxies=proxy,timeout=15)
            if response.status_code == 200:
                self.validip_que.put(validip)
                dir_name=time.strftime('%Y%m%d%H', time.localtime(time.time()))
                dir_path="C:/bee-ji/%s"%(dir_name)
                self.makeDir(dir_path,"列表目录")
                extension=response.headers.get("Content-Type").replace("image/","").replace("jpeg","jpg")
                if extension in ["jpg","gif","png","bmp","webp"]:
                    file_path="{}/{}.{}".format(dir_path,alt,extension)
                    with open(file_path,"wb") as f:
                        f.write(response.content)
                        end_time=time.time()
                        inter=end_time-start_time
                        print("%s成功下载图片%s，共花费%f秒"%(self.name,file_path,inter))
            else:
                raise StatusCodeError("%s状态码错误，返回状态码为%d"%(url,response.status_code))
        except BaseException as e:
            print("%s图片下载异常，错误信息为%s" % (self.name,str(e)))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s图片%s下载失败，正在进行第%d次重新下载!" % (self.name,url, repeat_count))
                self.downloadImage(url,repeat_count)
            else:
                print("%s图片%s下载失败，添加至数据库" % (self.name,url))
                self.sqlInsertFailedUrl(url,"image")

    def makeDir(self,dir_path,type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("%s成功创建%s%s" % (self.name,type,dir_path))
        except BaseException as e:
            print("%s创建%s异常，错误信息为%s"%(self.name,type,str(e)))

    def sqlInsertFailedUrl(self,url,type):
        try:
            global sql
            sql = """INSERT IGNORE INTO `bee-ji_failed_{}_url`(url) VALUES ('{}')""".format(type, url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("{}成功插入一条错误的{}记录到数据库".format(self.name,type))
        except BaseException as e:
            print("{}的sqlInsertFailedUrl抛出异常，异常内容为:{}".format(self.name,str(e)))


def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(100000)
    ipCheckoutThreadMount = 5
    ipCollectThreadMount = 1
    dataCollectThreadMount = 15
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(20)
    url_list = ["http://www.bee-ji.com/detail/%d.html"%(index) for index in range(20000,40000)]
    url_que = Queue(210000)
    for arc_url in url_list:
        url_que.put(arc_url)
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程%d" % (i), url_que, validip_que)
        worker.start()
        print("数据采集线程%d开启" % (i))
    url_que.join()

if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
