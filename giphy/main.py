# -*- coding:utf-8 -*-
import requests
import threading
from queue import Queue
import time
from proxy_helper import Proxy_helper
from userAgents import userAgents
import os
from tag_list import tag_list1
import StatusCodeError
from bs4 import BeautifulSoup
import redis
from TranslateUtils import TranslateUtils
from image_infos_collecter import ImageInfoCollecter
import proxy_helper
from StatusCodeError import StatusCodeError

class ImageDownLoader(threading.Thread):
    def __init__(self, threadName,validip_que,cn_img_info_que):
        threading.Thread.__init__(self)
        self.validip_que = validip_que
        self.daemon = True
        self.threadName=threadName
        self.validip_que=validip_que
        self.cn_img_info_que = cn_img_info_que
        self.userAgents =userAgents
        self.redis=redis.Redis('127.0.0.1', 6379)
        self.count=0

    def run(self):
        try:
            print("%s开始启动" % (self.threadName))
            self.makeDir("C:/giphy","下载根目录")
            global tag_list
            for tag in tag_list:
                self.makeDir("C:/giphy/{}".format(tag),"标签文件夹")
            while not self.cn_img_info_que.empty():
                cn_img_info = self.cn_img_info_que.get()
                self.dowmloadImage(cn_img_info)
                self.cn_img_info_que.task_done()
            else:
                print("中文图片信息队列为空，{}即将等待30秒".format(self.name))
                time.sleep(30)
                self.run()
        except BaseException as e:
            print("run函数抛出异常,异常内容为{}".format(str(e)))

    def dowmloadImage(self,cn_img_info,repeat_count=0):
        src = cn_img_info.split("********")[0]
        alt = cn_img_info.split("********")[1]
        tag = cn_img_info.split("********")[2]
        try:
            start_time=time.time()
            self.count+=1
            img_response = requests.get(src)
            dir_path="C:/giphy/{}".format(tag)
            if img_response.status_code == 200 or img_response.status_code == 304:
                file_extension = src.split("/")[-1].split(".")[-1]
                file_name="{}-{}.{}".format(alt,self.count,file_extension)
                file_path = "{}/{}" .format(dir_path,file_name)
                img_content = img_response.content
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName,file_path,end_time-start_time))
                    self.redis.sadd("success_download_img",cn_img_info)
            else:
                raise StatusCodeError("图片下载返回状态码出错，错误状态码为{}".format(img_response.status_code))
        except BaseException as e:
            print("图片下载抛出异常，异常内容为{}".format(str(e)))
            repeat_count += 1
            if repeat_count < 4:
                print("%s图片下载失败，正在进行第%d次重新下载!" % (src, repeat_count))
                self.dowmloadImage(cn_img_info, repeat_count)
            else:
                print("%s图片%s下载失败，将添加下载失败信息到数据表" % (self.threadName, cn_img_info))
                self.redis.sadd("failed_download_img",cn_img_info)

    def makeDir(self,dir_path,type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("成功创建%s%s" % (type,dir_path))
        except BaseException as e:
            print("创建%s异常，错误信息为%s"%(type,str(e)))


def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(10000)
    ipCollectThreadMount=2
    ipCheckoutThreadMount = 10
    imgInfosCollectThreadMount = 10
    imgDownloadThreadMount =50
    translateThreadMount =15
    imageSrcCheck_que = Queue(7000000)
    en_img_info_que = Queue(7000000)
    cn_img_info_que=Queue(7000000)
    block_que=Queue(1)
    tag_que = Queue(50000)
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(10)
    block_que.put(11111111111)
    for tag in tag_list:
        tag_que.put(tag)
    for i in range(imgInfosCollectThreadMount):
        worker = ImageInfoCollecter("图片信息采集线程%d" % (i),validip_que,tag_que,imageSrcCheck_que,en_img_info_que)
        worker.start()
    time.sleep(10)
    for i in range(translateThreadMount):
        worker = TranslateUtils("翻译线程%d" % (i), validip_que,en_img_info_que,cn_img_info_que)
        worker.start()
    time.sleep(10)
    for i in range(imgDownloadThreadMount):
        worker = ImageDownLoader("图片下载线程%d" % (i),validip_que, cn_img_info_que)
        worker.start()
    block_que.join()

if __name__ == "__main__":
    tag_list=tag_list1
    main()

