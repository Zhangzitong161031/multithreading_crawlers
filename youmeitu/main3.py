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
from lxml import etree
import urlList
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
        print("{}开始启动" .format(self.name))
        self.connectMysql()
        global mysqlInitialized
        global mysqlInitializeLock
        mysqlInitializeLock.acquire()
        if not mysqlInitialized:
            self.initializeMysql()
            mysqlInitialized = True
        mysqlInitializeLock.release()
        self.makeDir("C:/youmeitu","下载根目录")
        while not self.url_queue.empty():
            url = self.url_queue.get()
            self.getPaginationHtml(url)
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

    def getPaginationHtml(self,pageUrl,repeat_count=0):
        # validip = self.validip_que.get()
        # proxy = {'http': validip}
        try:
            response = requests.get(pageUrl,timeout=8)
            if response.status_code == 200:
                # self.validip_que.put(validip)
                category_list_str = pageUrl.replace("http://www.youmeitu.com/", "").replace(".html", "")
                pathArr = category_list_str.split("/")
                category = pathArr[0]
                list = pathArr[1] if len(pathArr) == 2 else "list_1"
                list_dir="C:/youmeitu/{}_{}".format(category,list)
                self.makeDir(list_dir,"栏目页文件夹")
                response.encoding = "utf-8"
                html = etree.HTML(response.text)
                arcLinkTags=html.xpath("//div[@class='TypeList'][1]//a[@class='TypeBigPics']")
                if len(arcLinkTags)==0:
                    self.sqlInsert(pageUrl, "failed_categoryList")
                for arcLinkTag in arcLinkTags:
                    arcUrl="http://www.youmeitu.com"+arcLinkTag.get("href")
                    self.getArcHtml(arcUrl,list_dir)
                self.sqlInsert(pageUrl,"complete_categoryList")
            else:
                raise StatusCodeError("状态码错误，状态码为{}".format(response.status_code))
        except BaseException as e:
            print("{}列表页下载异常，错误信息为{}" .format(self.threadName, str(e)))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("{}开始第{}次重新下载!" .format(self.name,repeat_count))
                self.getPaginationHtml(pageUrl, repeat_count)
            else:
                print("{}列表页\r\n{}添加到失败数据表" .format(self.threadName,pageUrl))
                self.sqlInsert(pageUrl,"failed_categoryList")

    def getArcHtml(self,arc_url,list_dir,repeat_count=0,flag=0):
        start_time=time.time()
        # validip = self.validip_que.get()
        # proxy = {'http': validip}
        try:
            response = requests.get(arc_url,timeout=8)
            if response.status_code == 200:
                # self.validip_que.put(validip)
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                img_list = soup.select('.ImageBody img')
                for img in img_list:
                    src = img.get("src")
                    file_extension_name=""
                    if ".jpg" in src:
                        src=src.split(".jpg")[0]+".jpg"
                        file_extension_name="jpg"
                    if ".jpeg" in src:
                        src=src.split(".jpg")[0]+".jpeg"
                        file_extension_name = "jpg"
                    if ".png" in src:
                        src=src.split(".png")[0]+".png"
                        file_extension_name = "png"
                    if ".gif" in src:
                        src=src.split(".gif")[0]+".gif"
                        file_extension_name = "gif"
                    if ".bmp" in src:
                        src=src.split(".bmp")[0]+".bmp"
                        file_extension_name = "bmp"
                    if "http" in src and "|||" not in src:
                        alt = img.get("alt")
                        invalid_str_arr = ["/", ".", "\\", "\r\n", "。", "*", '"', "<", ">", "|", "?", "？", ":"]
                        for invalid_str in invalid_str_arr:
                            alt = alt.replace(invalid_str, "")
                        file_name=alt+"."+file_extension_name
                        file_path =list_dir+"/"+file_name
                        self.downloadImage(src,file_path)
                #如果详情页有分页的情况
                if flag==0 and len(soup.select(".NewPages ul li"))!=0:
                    pageCount = int(soup.select(".NewPages ul li")[0].text.replace("共", "").replace("页: ", ""))
                    for i in range(1,pageCount+1):
                        pageurl = arc_url.replace(".html","")+"_"+str(i)+".html"
                        self.getArcHtml(pageurl,list_dir,repeat_count=0,flag=1)
            else:
                if response.status_code==404:
                    pass
                else:
                    raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("{}详情页下载异常，错误信息为{}" .format(self.threadName,str(e)))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("{}{}\r\n图片详情页开始第{}次重新下载!" .format(self.threadName,arc_url, repeat_count))
                self.getArcHtml(arc_url, list_dir, repeat_count,flag=flag)
            else:
                print("{}{}\r\n图片详情页下载失败" .format(self.threadName,arc_url))
                self.sqlInsert(arc_url,"failed_article")

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
            print("{}图片下载异常，错误信息为{}" .format(self.threadName,str(e)))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("{}图片{}下载抛出异常，正在进行第{}次重新下载!" .format(self.threadName,src, repeat_count))
                self.downloadImage(src, file_path, repeat_count)
            else:
                print("{}图片{}下载失败，将添加下载失败信息到数据表" .format(self.threadName,src))
                self.sqlInsert(src,"failed_image")

    def makeDir(self,dir_path,type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("{}成功创建{}{}" .format(self.threadName,type,dir_path))
        except BaseException as e:
            print("{}创建{}异常，错误信息为{}".format(self.threadName,type,str(e)))

    def sqlInsert(self,url,type):
        try:
            global sql
            sql = """INSERT IGNORE INTO youmeitu_{}_url(url) VALUES ('{}')""".format(type,url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("{}成功插入一条错误的{}记录到数据库".format(self.threadName,type))
        except BaseException as e:
            print("{}的sqlInsertFailedUrl抛出异常，异常内容为:{}".format(self.threadName,str(e)))


def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(100000)
    ipCheckoutThreadMount = 25
    ipCollectThreadMount = 3
    dataCollectThreadMount = 60
    # proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    # proxy_helper.run()
    # time.sleep(10)
    url_list=urlList.url_list1[2300:3000]
    url_que = Queue(5000)
    for arc_url in url_list:
        url_que.put(arc_url)
    print(url_que.qsize())
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程{}" .format(i), url_que, validip_que)
        worker.start()
        print("数据采集线程{}开启" .format(i))
    url_que.join()

if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()






