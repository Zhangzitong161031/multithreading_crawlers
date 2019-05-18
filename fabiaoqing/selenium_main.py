# -*- coding:utf-8 -*-
from selenium import webdriver
import time
from queue import Queue
import threading
import pymysql
from mysqlConfig import MysqlConfig
import os
import requests

class Image_src_collecter(threading.Thread):
    def __init__(self, threadName, url_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName=threadName
        self.mysqlConfig = MysqlConfig
        self.url_que = url_que
        self.productPageRequestCount=0
        self.failed_count = 0

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
            # 设置不加载图片
            chrome_opt = webdriver.ChromeOptions()
            # 不加载图片，设置的参数很固定
            prefs = {"profile.managed_default_content_settings.images": 2}
            chrome_opt.add_experimental_option("prefs", prefs)
            self.browser = webdriver.Chrome(options=chrome_opt)
            while not self.url_que.empty():
                url = self.url_que.get()
                self.start_work(url)
                self.url_que.task_done()
        except BaseException as e:
            print("run函数抛出异常")
            print(e)

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

    def start_work(self,url,repeat_count=0):
        self.makePageDir(url)
        try:
            self.browser.get(url)
            time.sleep(1)
            page_no = url.split("/")[-1].replace(".html", "")
            page_dir="D:/fabiaoqing/page" + page_no
            img_list = self.browser.find_elements_by_xpath("//img[@class='ui image lazy']")
            if len(img_list)==0:
                repeat_count+=1
                if repeat_count<3:
                    print("%s开始第%d次重新下载"%(url,repeat_count))
                    self.start_work(url,repeat_count)
                else:
                    print("%s下载失败，正在添加到失败列表数据表")
                    self.sqlInsertFailedListUrl(url)
            else:
                for img in img_list:
                    src = img.get_attribute('data-original')
                    start_time = time.time()
                    file_name = src.split("/")[-1]
                    file_path = page_dir + "/" + file_name
                    img_content = requests.get(src).content
                    with open(file_path, "wb") as f:
                        f.write(img_content)
                        end_time = time.time()
                        inter = end_time - start_time
                        print("成功下载图片%s，共花费%f秒" % (file_path, inter))
        except BaseException as e:
            print("refreshList函数抛出异常")
            print(e)
            self.sqlInsertFailedListUrl(url)

    def makePageDir(self,page_url):
        page_no = page_url.split("/")[-1].replace(".html", "")
        if not os.path.exists("D:/fabiaoqing/page" + page_no):
            os.mkdir("D:/fabiaoqing/page" + page_no)
            print("成功创建文件夹%s" % ("D:/fabiaoqing/page" + page_no))

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
     url_que = Queue(6000)
     dataCollectThreadMount=6
     url_list=["https://www.fabiaoqing.com/biaoqing/lists/page/%d.html"%(index) for index in range(600,4024)]
     for url in url_list:
         url_que.put(url)
     for i in range(dataCollectThreadMount):
         worker = Image_src_collecter("数据采集线程%d" % (i),url_que)
         worker.start()
     url_que.join()

if __name__=="__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
