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
        print("{}开始启动" .format(self.threadName))
        self.makeDir("C:/jiuwa","下载根目录")
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
            print("{}数据库连接成功".format(self.threadName))
        except Exception as e:
            print("{}数据库连接异常，错误信息为{}".format(self.threadName,str(e)))

    def initializeMysql(self):
        try:
            with open("initialize.sql",'r',encoding='utf-8') as fd:
                sqlStr=fd.read()
                sqlCommands = sqlStr.split(';')
                for command in sqlCommands:
                    if command!="":
                        self.mysqlClient.cursor().execute(command)
                        print("{}成功创建数据表{}".format(self.threadName,command.split("`")[1]))
                print('{}数据库初始化成功!'.format(self.threadName))
        except BaseException as e:
            print("{}数据库初始化异常，错误信息为{}".format(self.threadName,str(e)))


    def getListHtml(self,url,repeat_count=0):
        # validip = self.validip_que.get()
        # proxy = {'http': validip}
        try:
            response = requests.get(url,timeout=7)
            if response.status_code == 200:
                # self.validip_que.put(validip)
                page_no = url.replace("https://www.jiuwa.net/face/p-","")
                page_dir = "C:/jiuwa/index_" + page_no
                self.makeDir(page_dir,"列表文件夹")
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                a_list = soup.select(".title a")
                for a in a_list:
                    arc_url=a.get("href")
                    self.getArcHtml("https://www.jiuwa.net"+arc_url,page_dir)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("{}列表页下载异常，错误信息为{}，错误行号为{}" .format(self.threadName, str(e),e.__traceback__.tb_lineno))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("{}列表页{}下载失败，正在进行第{}次重新下载!" .format(self.threadName,url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("{}列表页{}下载失败，添加至数据库" .format(self.threadName,url))
                self.sqlInsertFailedUrl(url,"list")

    def getArcHtml(self,url,page_dir,pagination_no=1,repeat_count=0,pagination=False):
        start_time=time.time()
        # validip = self.validip_que.get()
        # proxy = {'http': validip}
        try:
            response = requests.get(url,timeout=7)
            if response.status_code == 200:
                # self.validip_que.put(validip)
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                title = soup.select(".title h1")[0].text.replace("|","").replace("/","").replace(r":","").replace("*","").replace("？","").replace("<","").replace(">","").replace('"',"").replace('\\',"")
                img_list = soup.select('.face-list img')
                invalid_str_arr = ["/", ".", "\\", "\r\n", "。", "*", '"', "<", ">", "|", "?", "？", ":"]
                for invalid_str in invalid_str_arr:
                    title = title.replace(invalid_str, "")
                index=1
                for img in img_list:
                    src=img.get("src")
                    file_extension_name = src.split("!")[0].split("/")[-1].split(".")[-1]
                    file_name = "{}-{}-{}.{}".format(title,pagination_no,index,file_extension_name)
                    file_path = page_dir + "/" + file_name
                    index+=1
                    self.downloadImage(src,file_path)
                # 如果存在分页
                pagination_list=soup.select('.am-pagination li a')
                if not pagination:
                    for index in range(1,len(pagination_list)):
                        pagination_no+=1
                        pagination_url="https://www.jiuwa.net"+pagination_list[index].get("href")
                        self.getArcHtml(pagination_url,page_dir,pagination_no,repeat_count=0,pagination=True)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("{}详情页{}下载异常，错误信息为{},错误行号为" .format(self.threadName,url, str(e)),e.__traceback__.tb_lineno)
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("{}表情详情页下载失败，正在进行第{}次重新下载!" .format(url, repeat_count))
                self.getArcHtml(url, page_dir,pagination_no,repeat_count,pagination)
            else:
                print("{}表情详情页下载失败" .format(url))
                self.sqlInsertFailedUrl(url,"article")

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
                    print("{}成功下载图片{}，共花费%f秒" .format(self.threadName,file_path, inter))
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("{}图片{}下载异常，错误信息为{},错误行号为{}" .format(self.threadName, src, str(e),e.__traceback__.tb_lineno))
            self.sqlInsertFailedUrl(src,"image")
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("{}图片下载抛出异常，正在进行第{}次重新下载!" .format(src, repeat_count))
                self.downloadImage(src, file_path, repeat_count)
            else:
                print("{}图片下载失败，将添加下载失败信息到数据表" .format(src))
                self.sqlInsertFailedUrl(src,"image")

    def makeDir(self,dir_path,type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("{}成功创建{}{}" .format(self.threadName,type,dir_path))
        except BaseException as e:
            print("{}创建{}异常，错误信息为{}".format(self.threadName,type,str(e)))

    def sqlInsertFailedUrl(self,url,type):
        try:
            global sql
            sql = """INSERT IGNORE INTO jiuwa_failed_{}_url(url) VALUES ('{}')""".format(type,url)
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
    ipCollectThreadMount = 4
    dataCollectThreadMount = 40
    # proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    # proxy_helper.run()
    # time.sleep(20)
    url_list=["https://www.jiuwa.net/face/p-{}".format(index) for index in range(1,128)]
    url_que = Queue(1000)
    for arc_url in url_list:
        url_que.put(arc_url)
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程{}" .format(i), url_que, validip_que)
        worker.start()
        print("数据采集线程{}开启" .format(i))
    url_que.join()

if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
