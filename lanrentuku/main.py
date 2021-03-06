# -*- coding:utf-8 -*-
import requests
import threading
from queue import Queue
import time
from proxy_helper import Proxy_helper
from bs4 import BeautifulSoup
import os
import pymysql
from mysqlConfig import MysqlConfig
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
        global mysqlInitialized
        global mysqlInitializeLock
        mysqlInitializeLock.acquire()
        if not mysqlInitialized:
            self.initializeMysql()
            mysqlInitialized=True
        mysqlInitializeLock.release()
        self.makeDir("C:/lanrentuku","下载根目录")
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

    def getListHtml(self,url,repeat_count=0):
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(url, proxies=proxy,timeout=5)
            if response.status_code == 200:
                self.validip_que.put(validip)
                page_no = url.split("%B1%ED%C7%E9%B0%FC/")[-1].replace("/", "")
                page_dir = "C:/lanrentuku/page" + page_no
                self.makeDir(page_dir, "列表文件夹")
                response.encoding = "gb2312"
                soup = BeautifulSoup(response.text, "lxml")
                a_list = soup.select(".list-qq dl dd a")
                for a in a_list:
                    arc_url="http://www.lanrentuku.com/%s"%(a.get("href"))
                    self.getArcHtml(arc_url,page_dir)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s列表页%s下载失败，正在进行第%d次重新下载!" % (self.threadName,url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("%s列表页%s下载失败，添加至数据库" % (self.threadName,url))
                self.sqlInsertFailedUrl(url,"list")

    def getArcHtml(self,url,page_dir,repeat_count=0):
        start_time=time.time()
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(url, proxies=proxy,timeout=5)
            if response.status_code == 200:
                self.validip_que.put(validip)
                response.encoding = "gb2312"
                soup = BeautifulSoup(response.text, "lxml")
                img_list = soup.select('.content-qq img')
                for img in img_list:
                    src = img.get("src")
                    alt = img.get("alt")
                    invalid_str_arr = ["/", ".", "\\", "\r\n", "。", "*", '"', "<", ">", "|", "?", "？", ":"]
                    for invalid_str in invalid_str_arr:
                        alt = alt.replace(invalid_str, "")
                    file_extension_name = src.split("!")[0].split("/")[-1].split(".")[-1]
                    file_name = alt + "." + file_extension_name
                    file_path = page_dir + "/" + file_name
                    self.downloadImage(src, file_path)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("%s详情页下载异常，错误信息为%s" % (self.threadName, str(e)))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s表情详情页下载失败，正在进行第%d次重新下载!" % (url, repeat_count))
                self.getArcHtml(url, page_dir, repeat_count)
            else:
                print("%s表情详情页下载失败" % (url))
                self.sqlInsertFailedUrl(url,"article")

    def downloadImage(self,src,file_path,repeat_count=0):
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            start_time=time.time()
            response = requests.get(src, proxies=proxy)
            if response.status_code==200:
                img_content=response.content
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    inter = end_time - start_time
                    print("成功下载图片%s，共花费%f秒" % (file_path, inter))
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("%s图片下载异常，错误信息为%s" % (self.threadName, str(e)))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s图片下载抛出异常，正在进行第%d次重新下载!" % (src, repeat_count))
                self.downloadImage(src, file_path, repeat_count)
            else:
                print("%s图片下载失败，将添加下载失败信息到数据表" % (src))
                self.sqlInsertFailedUrl(src,"image")

    def makeDir(self,dir_path,type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("%s成功创建%s%s" % (self.threadName,type,dir_path))
        except BaseException as e:
            print("%s创建%s异常，错误信息为%s"%(self.threadName,type,str(e)))

    def sqlInsertFailedUrl(self,url,type):
        try:
            global sql
            sql = """INSERT IGNORE INTO `lanrentuku_failed_{}_url`(url) VALUES ('{}')""".format(type, url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("{}成功插入一条错误的{}记录到数据库".format(self.name,type))
        except BaseException as e:
            print("{}的sqlInsertFailedUrl抛出异常，异常内容为:{}".format(self.name,str(e)))

def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(10000)
    ipCheckoutThreadMount = 20
    ipCollectThreadMount = 2
    dataCollectThreadMount =44
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(10)
    url_list = [
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/1/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/2/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/3/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/4/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/5/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/6/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/7/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/8/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/9/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/10/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/11/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/12/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/13/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/14/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/15/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/16/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/17/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/18/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/19/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/20/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/21/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/22/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/23/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/24/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/25/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/26/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/27/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/28/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/29/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/30/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/31/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/32/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/33/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/34/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/35/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/36/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/37/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/38/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/39/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/40/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/41/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/42/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/43/",
        "http://www.lanrentuku.com/sort/%B1%ED%C7%E9%B0%FC/44/"
    ]
    url_que = Queue(1000)
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
