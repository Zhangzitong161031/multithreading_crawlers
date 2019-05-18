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
from StatusCodeError import StatusCodeError

class Spider(threading.Thread):
    def __init__(self, threadName, url_queue, validip_que):
        threading.Thread.__init__(self)
        self.threadName=threadName
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
        self.makeDir("C:/doutula","下载根目录")
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
            response = requests.get(url,proxies=proxy,timeout=7)
            if response.status_code == 200:
                self.validip_que.put(validip)
                page_no = url.replace("https://www.doutula.com/article/list/?page=", "")
                page_dir="C:/doutula/page" + page_no
                self.makeDir(page_dir,"列表页文件夹")
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                a_list = soup.select(".list-group-item")
                for a in a_list:
                    arc_url=a.get("href")
                    if arc_url:
                        self.getArcHtml(arc_url,page_dir)
            else:
                raise StatusCodeError("状态码错误，状态码为%d"%(response.status_code))
        except BaseException as e:
            print("列表页下载异常，错误信息为%s" % (str(e)))
            repeat_count += 1
            if repeat_count < 4:
                print("%s列表页下载失败，正在进行第%d次重新下载!" % (url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("%s列表页下载失败" % (url))
                self.sqlInsertFailedUrl(url,"list")

    def getArcHtml(self,arc_url,page_dir,repeat_count=0):
        start_time=time.time()
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(arc_url,proxies=proxy,timeout=15)
            if response.status_code == 200:
                self.validip_que.put(validip)
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                img_list = soup.select('.pic-content img')
                for img in img_list:
                    src=img.get("src")
                    if src:
                        alt = img.get("alt")
                        invalid_str_arr = ["/", ".","\\", "\r\n","。","*", '"', "<", ">", "|", "?","？",":"]
                        for invalid_str in invalid_str_arr:
                            alt=alt.replace(invalid_str, "")
                        file_extension_name = src.split("/")[-1].split(".")[-1]
                        file_name = "{}.{}".format(alt,file_extension_name)
                        file_path = page_dir + "/" + file_name
                        if not os.path.exists(file_path):
                            self.downloadImage(src,file_path)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("{}详情页下载异常，错误信息为{}，所在行号为{}" .format (self.threadName,str(e),e.__traceback__.tb_lineno))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s详情页%s下载失败，正在进行第%d次重新下载!" % (self.threadName,arc_url, repeat_count))
                self.getArcHtml(arc_url, page_dir, repeat_count)
            else:
                print("%s详情页%s下载失败" % (self.threadName,arc_url))
                self.sqlInsertFailedUrl(arc_url,"article")

    def downloadImage(self,src,file_path,repeat_count=0):
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            start_time=time.time()
            response = requests.get(src,proxies=proxy)
            if response.status_code==200:
                img_content=response.content
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    inter = end_time - start_time
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName,file_path, inter))
            else:
                raise StatusCodeError("状态码错误，请求地址%s"%(src))
        except BaseException as e:
            print("{}图片{}下载异常，错误信息为{}，所在行号为{}".format(self.threadName,src,str(e), e.__traceback__.tb_lineno))
            self.validip_que.get(validip)
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
            sql = """INSERT IGNORE INTO `doutula_failed_{}_url`(url) VALUES ('{}')""".format(type, url)
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
    dataCollectThreadMount = 60
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(1)
    url_list = ["https://www.doutula.com/article/list/?page=%d"% (index) for index in range(320,603)]
    # url_list=[
    # "https://www.doutula.com/article/list/?page=197",
    # "https://www.doutula.com/article/list/?page=202",
    # "https://www.doutula.com/article/list/?page=206",
    # "https://www.doutula.com/article/list/?page=210",
    # "https://www.doutula.com/article/list/?page=213",
    # "https://www.doutula.com/article/list/?page=216",
    # "https://www.doutula.com/article/list/?page=217",
    # "https://www.doutula.com/article/list/?page=218",
    # "https://www.doutula.com/article/list/?page=219",
    # "https://www.doutula.com/article/list/?page=220",
    # "https://www.doutula.com/article/list/?page=221",
    # "https://www.doutula.com/article/list/?page=222",
    # "https://www.doutula.com/article/list/?page=224",
    # "https://www.doutula.com/article/list/?page=225",
    # "https://www.doutula.com/article/list/?page=231",
    # "https://www.doutula.com/article/list/?page=232",
    # "https://www.doutula.com/article/list/?page=233",
    # "https://www.doutula.com/article/list/?page=235",
    # "https://www.doutula.com/article/list/?page=236",
    # "https://www.doutula.com/article/list/?page=237",
    # "https://www.doutula.com/article/list/?page=238",
    # "https://www.doutula.com/article/list/?page=239",
    # "https://www.doutula.com/article/list/?page=241",
    # "https://www.doutula.com/article/list/?page=243",
    # "https://www.doutula.com/article/list/?page=244",
    # "https://www.doutula.com/article/list/?page=245",
    # "https://www.doutula.com/article/list/?page=246",
    # "https://www.doutula.com/article/list/?page=247",
    # "https://www.doutula.com/article/list/?page=248",
    # "https://www.doutula.com/article/list/?page=249",
    # "https://www.doutula.com/article/list/?page=250",
    # "https://www.doutula.com/article/list/?page=251",
    # "https://www.doutula.com/article/list/?page=252",
    # "https://www.doutula.com/article/list/?page=253",
    # "https://www.doutula.com/article/list/?page=254",
    # "https://www.doutula.com/article/list/?page=255",
    # "https://www.doutula.com/article/list/?page=256",
    # "https://www.doutula.com/article/list/?page=257",
    # "https://www.doutula.com/article/list/?page=258",
    # "https://www.doutula.com/article/list/?page=259",
    # "https://www.doutula.com/article/list/?page=260",
    # "https://www.doutula.com/article/list/?page=261",
    # "https://www.doutula.com/article/list/?page=262",
    # "https://www.doutula.com/article/list/?page=263",
    # "https://www.doutula.com/article/list/?page=265",
    # "https://www.doutula.com/article/list/?page=266",
    # "https://www.doutula.com/article/list/?page=267",
    # "https://www.doutula.com/article/list/?page=268",
    # "https://www.doutula.com/article/list/?page=269",
    # "https://www.doutula.com/article/list/?page=270",
    # "https://www.doutula.com/article/list/?page=271",
    # "https://www.doutula.com/article/list/?page=272",
    # "https://www.doutula.com/article/list/?page=273",
    # "https://www.doutula.com/article/list/?page=274",
    # "https://www.doutula.com/article/list/?page=275",
    # "https://www.doutula.com/article/list/?page=276",
    # "https://www.doutula.com/article/list/?page=277",
    # "https://www.doutula.com/article/list/?page=278",
    # "https://www.doutula.com/article/list/?page=279",
    # "https://www.doutula.com/article/list/?page=280",
    # "https://www.doutula.com/article/list/?page=281",
    # "https://www.doutula.com/article/list/?page=282",
    # "https://www.doutula.com/article/list/?page=283",
    # "https://www.doutula.com/article/list/?page=284",
    # "https://www.doutula.com/article/list/?page=285",
    # "https://www.doutula.com/article/list/?page=286",
    # "https://www.doutula.com/article/list/?page=287",
    # "https://www.doutula.com/article/list/?page=289",
    # "https://www.doutula.com/article/list/?page=290",
    # "https://www.doutula.com/article/list/?page=291",
    # "https://www.doutula.com/article/list/?page=292",
    # "https://www.doutula.com/article/list/?page=293",
    # "https://www.doutula.com/article/list/?page=295",
    # "https://www.doutula.com/article/list/?page=296",
    # "https://www.doutula.com/article/list/?page=297",
    # "https://www.doutula.com/article/list/?page=298",
    # "https://www.doutula.com/article/list/?page=299",
    # "https://www.doutula.com/article/list/?page=300",
    # "https://www.doutula.com/article/list/?page=301",
    # "https://www.doutula.com/article/list/?page=302",
    # "https://www.doutula.com/article/list/?page=303",
    # "https://www.doutula.com/article/list/?page=304",
    # "https://www.doutula.com/article/list/?page=305",
    # "https://www.doutula.com/article/list/?page=306",
    # "https://www.doutula.com/article/list/?page=307",
    # "https://www.doutula.com/article/list/?page=308",
    # "https://www.doutula.com/article/list/?page=309",
    # "https://www.doutula.com/article/list/?page=310",
    # "https://www.doutula.com/article/list/?page=311",
    # "https://www.doutula.com/article/list/?page=312",
    # "https://www.doutula.com/article/list/?page=313",
    # "https://www.doutula.com/article/list/?page=314",
    # "https://www.doutula.com/article/list/?page=315",
    # "https://www.doutula.com/article/list/?page=316",
    # "https://www.doutula.com/article/list/?page=317",
    # "https://www.doutula.com/article/list/?page=318",
    # "https://www.doutula.com/article/list/?page=319",
    # "https://www.doutula.com/article/list/?page=320",
    # "https://www.doutula.com/article/list/?page=321",
    # "https://www.doutula.com/article/list/?page=322",
    # "https://www.doutula.com/article/list/?page=323",
    # "https://www.doutula.com/article/list/?page=324",
    # "https://www.doutula.com/article/list/?page=325",
    # "https://www.doutula.com/article/list/?page=326",
    # "https://www.doutula.com/article/list/?page=327",
    # "https://www.doutula.com/article/list/?page=328",
    # "https://www.doutula.com/article/list/?page=329",
    # "https://www.doutula.com/article/list/?page=330",
    # "https://www.doutula.com/article/list/?page=331",
    # "https://www.doutula.com/article/list/?page=332",
    # "https://www.doutula.com/article/list/?page=333",
    # "https://www.doutula.com/article/list/?page=334",
    # "https://www.doutula.com/article/list/?page=335",
    # "https://www.doutula.com/article/list/?page=336",
    # "https://www.doutula.com/article/list/?page=337",
    # "https://www.doutula.com/article/list/?page=338",
    # "https://www.doutula.com/article/list/?page=339",
    # "https://www.doutula.com/article/list/?page=341",
    # "https://www.doutula.com/article/list/?page=342",
    # "https://www.doutula.com/article/list/?page=343",
    # "https://www.doutula.com/article/list/?page=345",
    # "https://www.doutula.com/article/list/?page=346",
    # "https://www.doutula.com/article/list/?page=347",
    # "https://www.doutula.com/article/list/?page=348",
    # "https://www.doutula.com/article/list/?page=349",
    # "https://www.doutula.com/article/list/?page=350",
    # "https://www.doutula.com/article/list/?page=351",
    # "https://www.doutula.com/article/list/?page=352",
    # "https://www.doutula.com/article/list/?page=353",
    # "https://www.doutula.com/article/list/?page=354",
    # "https://www.doutula.com/article/list/?page=356",
    # "https://www.doutula.com/article/list/?page=357",
    # "https://www.doutula.com/article/list/?page=358",
    # "https://www.doutula.com/article/list/?page=359",
    # "https://www.doutula.com/article/list/?page=361",
    # "https://www.doutula.com/article/list/?page=363",
    # "https://www.doutula.com/article/list/?page=364",
    # "https://www.doutula.com/article/list/?page=366",
    # "https://www.doutula.com/article/list/?page=367",
    # "https://www.doutula.com/article/list/?page=369",
    # "https://www.doutula.com/article/list/?page=370",
    # "https://www.doutula.com/article/list/?page=371",
    # "https://www.doutula.com/article/list/?page=373",
    # "https://www.doutula.com/article/list/?page=374",
    # "https://www.doutula.com/article/list/?page=375",
    # "https://www.doutula.com/article/list/?page=376",
    # "https://www.doutula.com/article/list/?page=379",
    # "https://www.doutula.com/article/list/?page=380",
    # "https://www.doutula.com/article/list/?page=381",
    # "https://www.doutula.com/article/list/?page=382",
    # "https://www.doutula.com/article/list/?page=383",
    # "https://www.doutula.com/article/list/?page=384",
    # "https://www.doutula.com/article/list/?page=385",
    # "https://www.doutula.com/article/list/?page=388",
    # "https://www.doutula.com/article/list/?page=390",
    # "https://www.doutula.com/article/list/?page=391",
    # "https://www.doutula.com/article/list/?page=392",
    # "https://www.doutula.com/article/list/?page=393",
    # "https://www.doutula.com/article/list/?page=394",
    # "https://www.doutula.com/article/list/?page=396",
    # "https://www.doutula.com/article/list/?page=400",
    # "https://www.doutula.com/article/list/?page=401",
    # "https://www.doutula.com/article/list/?page=402",
    # "https://www.doutula.com/article/list/?page=406",
    # "https://www.doutula.com/article/list/?page=407",
    # "https://www.doutula.com/article/list/?page=408",
    # "https://www.doutula.com/article/list/?page=411",
    # "https://www.doutula.com/article/list/?page=413",
    # "https://www.doutula.com/article/list/?page=414",
    # "https://www.doutula.com/article/list/?page=415",
    # "https://www.doutula.com/article/list/?page=416",
    # "https://www.doutula.com/article/list/?page=417",
    # "https://www.doutula.com/article/list/?page=418",
    # "https://www.doutula.com/article/list/?page=419",
    # "https://www.doutula.com/article/list/?page=420",
    # "https://www.doutula.com/article/list/?page=422",
    # "https://www.doutula.com/article/list/?page=424",
    # "https://www.doutula.com/article/list/?page=425",
    # "https://www.doutula.com/article/list/?page=426",
    # "https://www.doutula.com/article/list/?page=427",
    # "https://www.doutula.com/article/list/?page=428",
    # "https://www.doutula.com/article/list/?page=430",
    # "https://www.doutula.com/article/list/?page=432",
    # "https://www.doutula.com/article/list/?page=433",
    # "https://www.doutula.com/article/list/?page=434",
    # "https://www.doutula.com/article/list/?page=435",
    # "https://www.doutula.com/article/list/?page=436",
    # "https://www.doutula.com/article/list/?page=440",
    # "https://www.doutula.com/article/list/?page=442",
    # "https://www.doutula.com/article/list/?page=443",
    # "https://www.doutula.com/article/list/?page=444",
    # "https://www.doutula.com/article/list/?page=445",
    # "https://www.doutula.com/article/list/?page=446",
    # "https://www.doutula.com/article/list/?page=449",
    # "https://www.doutula.com/article/list/?page=450",
    # "https://www.doutula.com/article/list/?page=452",
    # "https://www.doutula.com/article/list/?page=453",
    # "https://www.doutula.com/article/list/?page=456",
    # "https://www.doutula.com/article/list/?page=458",
    # "https://www.doutula.com/article/list/?page=459",
    # "https://www.doutula.com/article/list/?page=460",
    # "https://www.doutula.com/article/list/?page=461",
    # "https://www.doutula.com/article/list/?page=462",
    # "https://www.doutula.com/article/list/?page=463",
    # "https://www.doutula.com/article/list/?page=464",
    # "https://www.doutula.com/article/list/?page=465",
    # "https://www.doutula.com/article/list/?page=466",
    # "https://www.doutula.com/article/list/?page=467",
    # "https://www.doutula.com/article/list/?page=468",
    # "https://www.doutula.com/article/list/?page=469",
    # "https://www.doutula.com/article/list/?page=470",
    # "https://www.doutula.com/article/list/?page=471",
    # "https://www.doutula.com/article/list/?page=472",
    # "https://www.doutula.com/article/list/?page=474",
    # "https://www.doutula.com/article/list/?page=475",
    # "https://www.doutula.com/article/list/?page=476",
    # "https://www.doutula.com/article/list/?page=477",
    # "https://www.doutula.com/article/list/?page=478",
    # "https://www.doutula.com/article/list/?page=481",
    # "https://www.doutula.com/article/list/?page=482",
    # "https://www.doutula.com/article/list/?page=483",
    # "https://www.doutula.com/article/list/?page=484",
    # "https://www.doutula.com/article/list/?page=486",
    # "https://www.doutula.com/article/list/?page=498",
    # "https://www.doutula.com/article/list/?page=501",
    # "https://www.doutula.com/article/list/?page=502",
    # "https://www.doutula.com/article/list/?page=503",
    # "https://www.doutula.com/article/list/?page=506",
    # "https://www.doutula.com/article/list/?page=508",
    # "https://www.doutula.com/article/list/?page=510",
    # "https://www.doutula.com/article/list/?page=511",
    # "https://www.doutula.com/article/list/?page=514",
    # "https://www.doutula.com/article/list/?page=516",
    # "https://www.doutula.com/article/list/?page=520",
    # "https://www.doutula.com/article/list/?page=521",
    # "https://www.doutula.com/article/list/?page=522",
    # "https://www.doutula.com/article/list/?page=523",
    # "https://www.doutula.com/article/list/?page=524",
    # "https://www.doutula.com/article/list/?page=525",
    # "https://www.doutula.com/article/list/?page=527",
    # "https://www.doutula.com/article/list/?page=528",
    # "https://www.doutula.com/article/list/?page=529",
    # "https://www.doutula.com/article/list/?page=531",
    # "https://www.doutula.com/article/list/?page=532",
    # "https://www.doutula.com/article/list/?page=533",
    # "https://www.doutula.com/article/list/?page=535",
    # "https://www.doutula.com/article/list/?page=537",
    # "https://www.doutula.com/article/list/?page=538",
    # "https://www.doutula.com/article/list/?page=540",
    # "https://www.doutula.com/article/list/?page=541",
    # "https://www.doutula.com/article/list/?page=542",
    # "https://www.doutula.com/article/list/?page=543",
    # "https://www.doutula.com/article/list/?page=544",
    # "https://www.doutula.com/article/list/?page=545",
    # "https://www.doutula.com/article/list/?page=547",
    # "https://www.doutula.com/article/list/?page=555",
    # "https://www.doutula.com/article/list/?page=557",
    # "https://www.doutula.com/article/list/?page=559",
    # "https://www.doutula.com/article/list/?page=561",
    # "https://www.doutula.com/article/list/?page=565",
    # "https://www.doutula.com/article/list/?page=566",
    # "https://www.doutula.com/article/list/?page=567",
    # "https://www.doutula.com/article/list/?page=569",
    # "https://www.doutula.com/article/list/?page=570",
    # "https://www.doutula.com/article/list/?page=571",
    # "https://www.doutula.com/article/list/?page=572",
    # "https://www.doutula.com/article/list/?page=573",
    # "https://www.doutula.com/article/list/?page=574",
    # "https://www.doutula.com/article/list/?page=578",
    # "https://www.doutula.com/article/list/?page=579"
    #
    # ]
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
    # pageDirArr=os.listdir("C:/doutula")
    # for pageDir in pageDirArr:
    #     pageFileArr=os.listdir("C:/doutula/"+pageDir)
    #     if len(pageFileArr)<10:
    #         print(pageDir.replace("page",""))




