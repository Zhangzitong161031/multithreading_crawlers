# -*- coding:utf-8 -*-
from selenium import webdriver
import time
from queue import Queue
import threading
import pymysql
from mysqlConfig import MysqlConfig
from selenium.webdriver.support.ui import WebDriverWait

mysqlInitializeLock = threading.Lock()
mysqlInitialized = False

class ImageInfoCollecter(threading.Thread):
    def __init__(self, threadName, imageInfo_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName=threadName
        self.mysqlConfig = MysqlConfig
        self.imageInfo_que = imageInfo_que
        self.productPageRequestCount=0
        self.failed_count = 0

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
        self.start_work()

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

    def start_work(self):
        try:
            # 设置不加载图片
            chrome_opt = webdriver.ChromeOptions()
            # 不加载图片，设置的参数很固定
            prefs = {"profile.managed_default_content_settings.images": 2}
            chrome_opt.add_experimental_option("prefs", prefs)
            browser = webdriver.Chrome(chrome_options=chrome_opt)
            wait=WebDriverWait(browser,10)
            browser.get("http://huaban.com")
            with open('cookie.txt', 'r') as f:
                cookieStr = f.read()
                cookiesArr = cookieStr.split(';')
            for cookieItem in cookiesArr:
                cookieItemName = cookieItem.split('=')[0]
                cookieItemValue = cookieItem.split('=')[1]
                browser.add_cookie({
                    'domain': '.huaban.com',
                    'name':cookieItemName,
                    'value':cookieItemValue
                })
            browser.get("http://huaban.com/favorite/illustration")
            for i in range(202000):
                print("开始第%d次下拉刷新"%(i+1))
                browser.execute_script("var q=document.documentElement.scrollTop=1000000")
                time.sleep(1.6)
                img_list=browser.find_elements_by_xpath("//div[@class='pin wfc ']/a/img[1]")
                for img in img_list:
                    src=img.get_attribute('src').replace("_fw236","")
                    alt = img.get_attribute("alt")
                    invalid_str_arr = ["/", ".", "\\", "\r\n", "。", "*", '"', "<", ">", "|", "?", "？", ":"]
                    for invalid_str in invalid_str_arr:
                        alt = alt.replace(invalid_str, "")
                    imgInfo="{}&&&&&&{}".format(src,alt)
                    self.imageInfo_que.put_unique(imgInfo)
                    self.sqlInsertImageUrl(imgInfo)

        except BaseException as e:
            print("start_work函数抛出异常，错误内容为%s"%(str(e)))

    def sqlInsertImageUrl(self,infos):
        try:
            global sql
            sql = """INSERT IGNORE INTO huaban_image_infos(infos) VALUES ('{}')""".format(infos)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
            else:
                pass
                # print("图片src已存在，插入到数据库失败")
        except BaseException as e:
            print("sqlInsertImageUrl抛出异常，错误信息:%s"%(str(e)))

def main():
     imageInfo_que = Queue(500000)
     dataCollectThreadMount=1
     for i in range(dataCollectThreadMount):
         worker = ImageInfoCollecter("图片地址采集线程%d" % (i),imageInfo_que)
         worker.start()

if __name__=="__main__":
    main()
