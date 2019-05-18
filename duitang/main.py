# -*- coding:utf-8 -*-
import requests
import threading
from queue import Queue
import re
import time
from proxy_helper import Proxy_helper
from bs4 import BeautifulSoup
from userAgents import userAgents
import os
import pymysql
from mysqlConfig import MysqlConfig
import math
from StatusCodeError import StatusCodeError

class Spider(threading.Thread):
    def __init__(self, threadName, url_queue, validip_que):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.daemon = True
        self.mysqlConfig = MysqlConfig
        self.url_queue = url_queue
        self.validip_que = validip_que
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
        self.makeDir("C:/duitang","下载根目录")
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
        #self.makePageDir(url)
        # validip = self.validip_que.get()
        # proxy = {'http': validip}
        try:
            response = requests.get(url,timeout=5)
            if response.status_code == 200:
                # self.validip_que.put(validip)
                response.encoding = "utf-8"
                start_no=int(url.split("&start=")[-1].replace("&_=1550562108374",""))
                page_no=math.floor(start_no/120)+1
                page_dir="C:/duitang/page"+str(page_no)
                self.makeDir(page_dir,"列表页根目录")
                imgInfos=re.findall(r'[\S\s\\s\r\n]{0,}?"path":"([\S\s]*?)","id"[\S\s\\s\r\n]{0,}?', response.text, re.I)
                for imgInfo in imgInfos:
                    start_time=time.time()
                    src=imgInfo.split('"},"msg":"')[0]
                    alt=imgInfo.split('"},"msg":"')[-1]
                    invalid_str_arr = ["/", ".", "\\", "\r\n", "。", "*", '"', "<", ">", "|", "?", "？", ":"]
                    for invalid_str in invalid_str_arr:
                        alt = alt.replace(invalid_str, "")
                    if not alt:
                        alt=src.split("/")[-1].split(".")[0]
                    alt=alt[0:16]
                    file_extension_name = src.split("/")[-1].split(".")[-1].replace("jpeg","jpg")
                    file_name = alt + "." + file_extension_name
                    file_path = page_dir + "/" + file_name
                    if not os.path.exists(file_path):
                        self.downloadImage(src,file_path)
            else:
                raise StatusCodeError("状态码错误,状态码为{}".format(response.status_code))
        except BaseException as e:
            print("列表页下载异常，错误信息为{},错误行号为{}" .format (str(e),e.__traceback__.tb_lineno))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s列表页下载失败，正在进行第%d次重新下载!" % (url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("%s列表页下载失败，添加至数据库" % (url))
                self.sqlInsertFailedUrl(url, "list")

    def downloadImage(self,src,file_path,repeat_count=0):
        # validip = self.validip_que.get()
        # proxy = {'http': validip}
        try:
            start_time=time.time()
            response = requests.get(src)
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
            print("{}图片下载异常，错误信息为{}，行号为{}" .format(self.threadName,str(e),e.__traceback__.tb_lineno))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s图片%s下载抛出异常，正在进行第%d次重新下载!" % (self.threadName,src, repeat_count))
                self.downloadImage(src, file_path, repeat_count)
            else:
                print("%s图片%s下载失败，将添加下载失败信息到数据表" % (self.threadName,src))
                self.sqlInsertFailedUrl(src,"image")

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
            sql = """INSERT IGNORE INTO duitang_failed_{}_url(url) VALUES ('{}')""".format(type,url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("{}成功插入一条错误的{}记录到数据库".format(self.threadName,type))
        except BaseException as e:
            print("{}sqlInsertFailedUrl抛出异常，异常内容为:{}".format(self.threadName,str(e)))

def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(10000)
    ipCheckoutThreadMount = 20
    ipCollectThreadMount = 2
    dataCollectThreadMount = 50
    # proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    # proxy_helper.run()
    # time.sleep(10)
    url_list = ["https://www.duitang.com/napi/blog/list/by_filter_id/?include_fields=top_comments%2Cis_root%2Csource_link%2Citem%2Cbuyable%2Croot_id%2Cstatus%2Clike_count%2Csender%2Calbum%2Creply_count&filter_id=%E6%8F%92%E7%94%BB%E7%BB%98%E7%94%BB&start={}&_=1550562108374".format((index-1)*24) for index in range(200,1001)]
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
