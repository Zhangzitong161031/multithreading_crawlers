# -*- coding:gb2312 -*-
import requests
import threading
from queue import Queue
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
        self.count = 0

    def run(self):
        print("%s��ʼ����" % (self.name))
        self.connectMysql()
        global mysqlInitialized
        global mysqlInitializeLock
        mysqlInitializeLock.acquire()
        if not mysqlInitialized:
            self.initializeMysql()
            mysqlInitialized=True
        mysqlInitializeLock.release()
        self.makeDir("C:/fabiaoqing","���ظ�Ŀ¼")
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
            print("%s���ݿ����ӳɹ�" % (self.threadName))
        except Exception as e:
            print("%s���ݿ������쳣��������ϢΪ%s" % (self.threadName, str(e)))

    def initializeMysql(self):
        try:
            with open("initialize.sql", 'r', encoding='utf-8') as fd:
                sqlStr = fd.read()
                sqlCommands = sqlStr.split(';')
                for command in sqlCommands:
                    if command != "":
                        self.mysqlClient.cursor().execute(command)
                        print("{}�ɹ��������ݱ�{}".format(self.threadName, command.split("`")[1]))
                print('%s���ݿ��ʼ���ɹ�!' % (self.threadName))
        except BaseException as e:
            print("%s���ݿ��ʼ���쳣��������ϢΪ%s" % (self.threadName, str(e)))

    def getListHtml(self,url,repeat_count=0):
        # validip = self.validip_que.get()
        # proxy = {'http': validip}
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Host": "www.fabiaoqing.com",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "%s" % (self.userAgents[self.count % 17])
        }
        try:
            response = requests.get(url,headers=headers,timeout=9)
            if response.status_code == 200:
                # self.validip_que.put(validip)
                page_no = url.split("/")[-1].replace(".html", "")
                page_dir ="C:/fabiaoqing/page" + page_no
                self.makeDir(page_dir,"�б�ҳ�ļ���")
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                img_list = soup.select(".lazy")
                for img in img_list:
                    start_time = time.time()
                    src=img.get("data-original")
                    alt = img.get("alt")
                    invalid_str_arr = ["/", ".", "\\", "\r\n", "��", ":", "*", "��", '"', "<", ">", "|", "?", "?"]
                    for invalid_str in invalid_str_arr:
                        alt.replace(invalid_str, "")
                    file_extension_name = src.split("/")[-1].split(".")[-1]
                    file_name = "{}.{}".format(alt, file_extension_name)
                    file_path = page_dir + "/" + file_name
                    self.downloadImage(src,file_path)
            else:
                raise StatusCodeError("״̬�����")
        except BaseException as e:
            print("%s�б�ҳ�����쳣��������ϢΪ%s" % (self.threadName,str(e)))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s�б�ҳ%s����ʧ�ܣ����ڽ��е�%d����������!" % (self.threadName,url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("%s�б�ҳ%s����ʧ�ܣ���������ݿ�" % (self.threadName,url))
                self.sqlInsertFailedUrl(url,"list")

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
                    print("%s�ɹ�����ͼƬ%s��������%f��" % (self.threadName,file_path, inter))
            else:
                raise StatusCodeError("״̬�����")
        except BaseException as e:
            print("%sͼƬ�����쳣��������ϢΪ%s" % (self.threadName,str(e)))
            # self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%sͼƬ%s�����׳��쳣�����ڽ��е�%d����������!" % (self.threadName,src, repeat_count))
                self.downloadImage(src, file_path, repeat_count)
            else:
                print("%sͼƬ%s����ʧ�ܣ����������ʧ����Ϣ�����ݱ�" % (self.threadName,src))
                self.sqlInsertFailedUrl(src,"image")

    def makeDir(self,dir_path,type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("%s�ɹ�����%s%s" % (self.threadName,type,dir_path))
        except BaseException as e:
            print("%s����%s�쳣��������ϢΪ%s"%(self.threadName,type,str(e)))

    def sqlInsertFailedUrl(self,url,type):
        try:
            global sql
            sql = """INSERT IGNORE INTO fabiaoqing_failed_{}_url(url) VALUES ('{}')""".format(type,url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("{}�ɹ�����һ�������{}��¼�����ݿ�".format(self.threadName,type))
        except BaseException as e:
            print("{}sqlInsertFailedUrl�׳��쳣���쳣����Ϊ:{}".format(self.threadName,str(e)))

def main():
    # �������̲߳ɼ�����IP���������ڴ���IP�Ķ���ipproxy_que��
    ip_que = Queue(1200)
    validip_que = Queue(80000)
    ipCheckoutThreadMount = 22
    ipCollectThreadMount = 2
    dataCollectThreadMount =30
    # proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    # proxy_helper.run()
    # time.sleep(15)
    url_list = ["https://www.fabiaoqing.com/biaoqing/lists/page/%d.html"%(index) for index in range(1,4025)]
    url_que = Queue(5000)
    url_que.put("http://www.qqjia.com/biaoqing/")
    for arc_url in url_list:
        url_que.put(arc_url)
    for i in range(dataCollectThreadMount):
        worker = Spider("���ݲɼ��߳�%d" % (i), url_que, validip_que)
        worker.start()
        print("���ݲɼ��߳�%d����" % (i))
    url_que.join()

if __name__ == "__main__":
    mysqlInitializeLock = threading.Lock()
    mysqlInitialized = False
    main()
