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
from lxml import etree
from StatusCodeError import StatusCodeError

class Spider(threading.Thread):
    def __init__(self,threadName,url_queue,validip_que):
        threading.Thread.__init__(self)
        self.threadName=threadName
        self.daemon = True
        self.mysqlConfig = MysqlConfig
        self.url_queue = url_queue
        self.validip_que = validip_que

    def run(self):
        print("{}开始启动" .format(self.name))
        self.makeDir("C:/sj33","下载根目录")
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
            response = requests.get(url,timeout=10)
            # self.validip_que.put(validip)
            if response.status_code == 200:
                page_no = url.replace("http://www.sj33.cn/cg/chys/", "").replace(".html", "")
                page_dir="C:/sj33/page{}".format(page_no)
                self.makeDir("C:/sj33/page{}".format(page_no),"列表文件夹")
                response.encoding = "utf-8"
                html = etree.HTML(response.text)
                page_no=int(url.replace("http://www.sj33.cn/cg/chys/P","").replace(".html",""))

                if page_no < 219:
                    # 列表页在219页以下的使用以下规则
                    a_list = list(set(html.xpath('//div[@id="typelink3"]/a[1]')))

                else:
                    # 列表页在219页以上的使用以下规则
                    a_list = list(set(html.xpath('//ul[@class="imglist"]/li/a[1]')))

                for a in a_list:
                    arc_url="http://www.sj33.cn"+a.get("href")
                    self.getArcHtml(arc_url,page_dir)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            # self.validip_que.get(validip)
            print("{}请求列表页异常，错误信息为{}，行号为{}".format(self.threadName,str(e),e.__traceback__.tb_lineno))
            repeat_count += 1
            if repeat_count < 4:
                print("{}列表页{}下载失败，正在进行第{}次重新下载!" .format(self.threadName, url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("{}列表页{}下载失败" .format(self.threadName, url))
                self.sqlInsertFailedUrl(url, "list")


    def getArcHtml(self,url,page_dir,repeat_count=0,flag=0):
        start_time=time.time()
        # validip = self.validip_que.get()
        # proxy = {'http': validip}
        try:
            response = requests.get(url,timeout=5)
            if response.status_code == 200:
                # self.validip_que.put(validip)
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text,"lxml")
                img_list = soup.select('.articlebox img')
                index=0
                for img in img_list:
                    index+=1
                    src=img.get("src")
                    title = img.get("title")
                    invalid_str_arr = ["/", ".", "\\", "\r\n", "。", ":", "*", "：", '"', "<", ">", "|", "?", "�"]
                    for invalid_str in invalid_str_arr:
                        title.replace(invalid_str, "")
                    file_extension_name = src.split("/")[-1].split(".")[-1]
                    file_name = "{}-{}.{}".format(title, index,file_extension_name)
                    file_path = page_dir + "/" + file_name
                    self.downloadImage(src,file_path)
            else:
                repeat_count += 1
                if repeat_count < 4:
                    print("{}详情页下载失败，正在进行第{}次重新下载!" .format(url,repeat_count))
                    self.getArcHtml(url,page_dir,repeat_count,flag=flag)
                else:
                    print("{}详情页下载失败" .format(url))
                    self.sqlInsertFailedUrl(url,"article")
        except BaseException as e:
            print("请求详情页异常，错误信息为{}" .format(str(e)))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("{}详情页下载失败，正在进行第{}次重新下载!" .format(url,repeat_count))
                self.getArcHtml(url,page_dir,repeat_count,flag=flag)
            else:
                print("{}详情页下载失败" .format(url))
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
                    print("{}成功下载图片{}，共花费{}秒" .format(self.threadName,file_path, inter))
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("{}图片下载异常，错误信息为{}，行号为" .format(self.threadName,str(e),e.__traceback__.tb_lineno))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("{}图片{}下载抛出异常，正在进行第{}次重新下载!" .format(self.threadName,src, repeat_count))
                self.downloadImage(src, file_path, repeat_count)
            else:
                print("{}图片{}下载失败，将添加下载失败信息到数据表" .format(self.threadName,src))
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
            sql = """INSERT IGNORE INTO fabiaoqing_failed_{}_url(url) VALUES ('{}')""".format(type,url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("{}成功插入一条错误的{}记录到数据库".format(self.threadName,type))
        except BaseException as e:
            print("{}sqlInsertFailedUrl抛出异常，异常内容为:{}".format(self.threadName,str(e)))


def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(100000)
    ipCheckoutThreadMount = 30
    ipCollectThreadMount = 4
    dataCollectThreadMount = 50
    # proxy_helper = Proxy_helper(ip_que,validip_que,ipCheckoutThreadMount,ipCollectThreadMount)
    # proxy_helper.run()
    # time.sleep(20)
    url_list = ["http://www.sj33.cn/cg/chys/P{}.html".format(index) for index in range(1,239)]
    url_que = Queue(600)
    for arc_url in url_list:
        url_que.put(arc_url)
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程{}" .format(i),url_que,validip_que)
        worker.start()
        print("数据采集线程{}开启" .format(i))
    url_que.join()


if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
