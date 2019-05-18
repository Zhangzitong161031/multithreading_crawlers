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

class Spider(threading.Thread):
    def __init__(self, threadName, mgUrl_que,validip_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName=threadName
        self.validip_que=validip_que
        self.mysqlConfig = MysqlConfig
        self.imgUrl_que = mgUrl_que
        self.userAgents =userAgents
        self.productPageRequestCount=0
        self.count = 0

    def run(self):
        try:
            print("%s开始启动" % (self.name))
            self.connectMysql()
            global mysqlInitialized
            global mysqlInitializeLock
            mysqlInitializeLock.acquire()
            if not mysqlInitialized:
                self.initializeMysql()
                mysqlInitialized=True
            mysqlInitializeLock.release()
            self.makeRootDir()
            while not self.imgUrl_que.empty():
                url = self.imgUrl_que.get()
                self.dowmloadImage(url)
                self.imgUrl_que.task_done()
        except BaseException as e:
            print("run函数抛出异常")
            print(e)

    def makeRootDir(self):
        try:
            if not os.path.exists("E:/huaban"):
                os.mkdir("E:/huaban")
                print("成功创建文件夹E:/huaban")
        except BaseException as e:
            print("makeRootDir抛出异常")
            print(e)

    def checkFileNotExisted(self,src):
        file_firstName = src.split("/")[-1]
        file_save_name = "E:/huaban/%s" % (file_firstName)
        extensionsArr = [".jpg", ".webp", ".gif", ".png", ".jpeg", ".bmp"]
        file_not_exist = True
        for extension in extensionsArr:
            if os.path.exists("%s%s" % (file_save_name, extension)):
                file_not_exist = False
                print("图片%s已存在"%("%s%s"%(file_save_name,extension)))
                break
        return file_not_exist

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
            self.initializeImageRequestHeaders()
            validip = self.validip_que.get()
            proxy = {'http': validip}
            img_response = requests.get(src, proxies=proxy,headers=self.image_headers)
            if img_response.status_code == 200 or img_response.status_code == 304:
                file_firstName = src.split("/")[-1]
                file_save_name = "E:/huaban/%s" % (file_firstName)
                self.validip_que.put(validip)
                img_extension = img_response.headers.get("Content-Type").replace("image/", "").replace("jpeg", "jpg")
                file_path = "%s.%s" % (file_save_name, img_extension)
                img_content = img_response.content
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    self.sqlInsertCompleteImageUrl(src)
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName,file_path,end_time-start_time))
                    time.sleep(1)
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
            sql = """INSERT IGNORE INTO huaban_complete_imageurl(url) VALUES ('{}')""".format(url)
            self.mysqlClient.cursor().execute(sql)
            self.mysqlClient.commit()
            print("成功添加一条已完成的Url到数据库%s" % (url))
        except BaseException as e:
            print("数据插入失败")

def main(url_list):
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(10000)
    ipCheckoutThreadMount = 36
    ipCollectThreadMount = 5
    dataCollectThreadMount =100
    url="http://huaban.com/favorite/illustration"
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(10)
    url_list=url_list
    imgurl_que = Queue(50000)
    for arc_url in url_list:
        imgurl_que.put(arc_url)
    # browser_helper = Browser_helper(url,imgurl_que)
    # browser_helper.run()
    for i in range(dataCollectThreadMount):
        worker = Spider("数据采集线程%d" % (i), imgurl_que,validip_que)
        worker.start()
        print("数据采集线程%d开启" % (i))
    imgurl_que.join()

if __name__ == "__main__":
    # mysqlInitializeLock = threading.Lock()
    # mysqlInitialized = False
    # url_list = url_list.url_list
    # main(url_list)

    fileNameArr=os.listdir("C:/gfycat")
    imgurl_que = Queue(1)
    validip_que = Queue(1)
    spider = Spider("111", imgurl_que, validip_que)
    spider.connectMysql()
    for fileName in fileNameArr:
        fileSrc="https://thumbs.gfycat.com/"+fileName
        try:
            global sql
            sql = """INSERT IGNORE INTO gfycat_complete_imageurl (url) VALUES ('{}')""".format(fileSrc)
            spider.mysqlClient.cursor().execute(sql)
            spider.mysqlClient.commit()
            print("成功添加一条已完成的Url到数据库%s" % (fileSrc))
        except BaseException as e:
            print(e)