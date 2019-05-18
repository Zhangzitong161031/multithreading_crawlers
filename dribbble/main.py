# -*- coding:gb2312 -*-
import requests
import threading
from queue import Queue
import time
import os
import pymysql
from mysqlConfig import MysqlConfig
import re
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
        print("%s��ʼ����" % (self.name))
        self.makeDir("D:/dribbble","���ظ�Ŀ¼")
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
        try:
            response = requests.get(url,timeout=20)
            if response.status_code == 200:
                response.encoding = "utf-8"
                imgurls = re.findall(r'<picture>[\S\s\\s\r\n]{0,}?<source srcset="([\S\s\\s\r\n]{0,}?)" media=', response.text, re.I)
                for imgurl in imgurls:
                    file_name = imgurl.split("/")[-1]
                    file_path ="D:/dribbble/" + file_name
                    self.downloadImage(imgurl,file_path)
            else:
                raise StatusCodeError("״̬�����")
        except BaseException as e:
            print("�б�ҳ�����쳣��������ϢΪ%s" % (str(e)))
            repeat_count += 1
            if repeat_count < 4:
                print("%s�б�ҳ����ʧ�ܣ����ڽ��е�%d����������!" % (url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("%s�б�ҳ����ʧ�ܣ���������ݿ�" % (url))
                self.sqlInsertFailedUrl(url,"list")

    def downloadImage(self,src,file_path,repeat_count=0):
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            start_time=time.time()
            response = requests.get(src, proxies=proxy)
            if response.status_code==200:
                img_content=response.text
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    inter = end_time - start_time
                    print("%s�ɹ�����ͼƬ%s��������%f��" % (self.threadName,file_path, inter))
            else:
                raise StatusCodeError("״̬�����")
        except BaseException as e:
            print("%sͼƬ�����쳣��������ϢΪ%s" % (self.threadName,str(e)))
            self.validip_que.get(validip)
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
                print("�ɹ�����%s%s" % (type,dir_path))
        except BaseException as e:
            print("����%s�쳣��������ϢΪ%s"%(type,str(e)))

    def sqlInsertFailedUrl(self,url,type):
        try:
            global sql
            sql = """INSERT IGNORE INTO `bee-ji_failed_{}_url`(url) VALUES ('{}')""".format(type,url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("%s�ɹ�����һ�������{}��¼�����ݿ�".format(self.name,type))
        except BaseException as e:
            print("{}��sqlInsertFailedUrl�׳��쳣���쳣����Ϊ:{}".format(self.name,str(e)))

def main():
    # �������̲߳ɼ�����IP���������ڴ���IP�Ķ���ipproxy_que��
    ip_que = Queue(1200)
    validip_que = Queue(80000)
    ipCheckoutThreadMount = 22
    ipCollectThreadMount = 2
    dataCollectThreadMount =25
    # proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    # proxy_helper.run()
    # time.sleep(15)
    url_list = ["https://dribbble.com/shots?page=%d"%(index) for index in range(1,26)]
    url_que = Queue(100)
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
