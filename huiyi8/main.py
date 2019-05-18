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
        self.threadName=threadName
        self.daemon = True
        self.mysqlConfig = MysqlConfig
        self.url_queue = url_queue
        self.validip_que = validip_que

    def run(self):
        print("%s开始启动" % (self.name))
        self.connectMysql()
        self.initializeMysql()
        self.makeDir("C:/huiyi8","下载根目录")
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
                page_no=url.replace("https://www.huiyi8.com/qqbq/","").replace(".html","")
                page_dir="C:/huiyi8/page{}".format(page_no)
                self.makeDir(page_dir,"列表文件夹")
                self.validip_que.put(validip)
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                a_list = soup.select(".works-img-box")
                for a in a_list:
                    arc_url=a.get("href")
                    self.getArcHtml(arc_url,page_dir)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("%s列表页下载异常，错误信息为%s" % (self.threadName,str(e)))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s列表页%s下载失败，正在进行第%d次重新下载!" % (self.threadName,url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("%s列表页%s下载失败，添加至数据库" % (self.threadName,url))
                self.sqlInsertFailedUrl(url, "list")

    def getArcHtml(self,arc_url,page_dir,repeat_count=0):
        start_time=time.time()
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(arc_url, proxies=proxy,timeout=5)
            if response.status_code == 200:
                self.validip_que.put(validip)
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                title = soup.select(".workTitle")[0].text.replace("|","").replace("/","").replace(r":","").replace("*","").replace("？","").replace("<","").replace(">","").replace('"',"").replace('\\',"")
                invalid_str_arr = ["/", ".", "\\", "\r\n", "。", "*", '"', "<", ">", "|", "?", "？", ":"]
                for invalid_str in invalid_str_arr:
                    title = title.replace(invalid_str, "")
                img_list = soup.select('.imgcont img')
                index=0
                for img in img_list:
                    index+=1
                    src=img.get("src")
                    file_extension_name = src.split("!")[0].split("/")[-1].split(".")[-1]
                    file_path = "{}/{}-{}.{}".format(page_dir,title,index,file_extension_name)
                    self.downloadImage(src,file_path)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("%s详情页下载异常，错误信息为%s" % (self.threadName,str(e)))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s详情页%s下载失败，正在进行第%d次重新下载!" % (self.threadName,arc_url, repeat_count))
                self.getArcHtml(arc_url,repeat_count)
            else:
                print("%s详情页%s下载失败" % (self.threadName,arc_url))
                self.sqlInsertFailedUrl(arc_url,"article")

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
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName,file_path, inter))
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("%s图片下载异常，错误信息为%s" % (self.threadName,str(e)))
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
            sql = """INSERT IGNORE INTO huiyi8_failed_{}_url(url) VALUES ('{}')""".format(type,url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("%s成功插入一条错误的{}记录到数据库".format(self.threadName,type))
        except BaseException as e:
            print("%s的sqlInsertFailedUrl抛出异常，异常内容为:{}".format(self.threadName,str(e)))


def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(10000)
    ipCheckoutThreadMount = 50
    ipCollectThreadMount = 2
    dataCollectThreadMount =89
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(5)
    url_list = ["https://www.huiyi8.com/qqbq/{}.html".format(index) for index in range(1,20)]
    url_que = Queue(1000)
    for url in url_list:
        url_que.put(url)
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程%d" % (i), url_que, validip_que)
        worker.start()
        print("数据采集线程%d开启" % (i))
    url_que.join()

if __name__ == "__main__":
    main()
