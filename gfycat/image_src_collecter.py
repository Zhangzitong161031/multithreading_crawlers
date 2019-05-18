# -*- coding:utf-8 -*-
from selenium import webdriver
import time
from queue import Queue
import threading
import pymysql
from mysqlConfig import MysqlConfig

class Image_src_collecter(threading.Thread):
    def __init__(self, threadName, cate_url_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName=threadName
        self.mysqlConfig = MysqlConfig
        self.cate_url_que = cate_url_que
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
            while not self.cate_url_que.empty():
                cate_url = self.cate_url_que.get()
                self.start_work(cate_url)
                self.cate_url_que.task_done()
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

    def start_work(self,cate_url):
        try:
            # 设置不加载图片
            chrome_opt = webdriver.ChromeOptions()
            # 不加载图片，设置的参数很固定
            prefs = {"profile.managed_default_content_settings.images": 2}
            chrome_opt.add_experimental_option("prefs", prefs)
            browser = webdriver.Chrome(chrome_options=chrome_opt)
            browser.get(cate_url)
            tag=cate_url.replace("https://gfycat.com/gifs/tag/","")
            for i in range(1,2000):
                if self.failed_count<300:
                    browser.execute_script("var q=document.documentElement.scrollTop=1000000")
                    time.sleep(0.6)
                    img_list=browser.find_elements_by_xpath("//div[@class='m-grid-item']//picture/img[@class='image media'][1]")
                    \for img in img_list:
                        src=img.get_attribute('src')
                        self.sqlInsertImageUrl(src,tag)
                else:
                    print("%s采集线程的数据插入失败次数已达上限，即将跳出循环"%(tag))
                    self.sqlInsertCompleteCate(tag)
                    self.failed_count=0
                    break
            browser.close()
        except BaseException as e:
            print("refreshList函数抛出异常")
            print(e)
            self.sqlInsertFailedListUrl(cate_url)

    def sqlInsertImageUrl(self, url,tag):
        try:
            global sql
            tag=tag.replace("'",'')
            sql = """INSERT IGNORE INTO gfycat_imageurl(url,tag) VALUES ('{}','{}')""".format(url,tag)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("%s成功插入一条图片路径"%(tag))
            else:
                self.failed_count += 1
                print("图片路径已存在，%s插入失败累积次数%d"%(tag,self.failed_count))
        except BaseException as e:
            print("sqlInsertImageUrl抛出异常，图片路径插入失败")
            print(e)
            print("url为%s,tag为%s"%(url,tag))

    def sqlInsertCompleteCate(self,tag):
        try:
            global sql
            tag = tag.replace("'", '')
            sql = """INSERT IGNORE INTO gfycat_complete_cate(tag) VALUES ('{}')""".format(tag)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("成功插入一条已完成的列表记录%s"%(tag))
            else:
                print("已完成的列表记录%s已存在，插入失败"%(tag))
        except BaseException as e:
            print("sqlInsertCompleteCate抛出异常，已完成的列表记录%s插入失败"%(tag))
            print(e)
    def sqlInsertFailedListUrl(self, url):
        try:
            global sql
            sql = """INSERT IGNORE INTO gfycat_failed_list(url) VALUES ('{}')""".format(url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("成功插入一条失败列表记录")
            else:
                print("失败列表记录已存在，插入失败")
        except BaseException as e:
            print("sqlInsertFailedListUrl抛出异常,失败列表记录插入失败")
            print(e)

def main():
     cate_url_que = Queue(350)
     cate_url_list = [
     "https://gfycat.com/gifs/tag/celebs",
     ]
     dataCollectThreadMount=1
     for url in cate_url_list:
         cate_url_que.put(url)
     for i in range(dataCollectThreadMount):
         worker = Image_src_collecter("数据采集线程%d" % (i),cate_url_que)
         worker.start()
     cate_url_que.join()

if __name__=="__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
