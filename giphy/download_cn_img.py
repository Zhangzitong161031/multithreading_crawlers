# -*- coding:utf-8 -*-
import requests
import threading
from queue import Queue
import time
import os
import StatusCodeError
import redis
from StatusCodeError import StatusCodeError

class ImageDownLoader(threading.Thread):
    def __init__(self, threadName):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName = threadName
        self.redis = redis.Redis('127.0.0.1', 6379, decode_responses=True)
        self.count = 0

    def run(self):
        try:
            print("%s开始启动" % (self.threadName))
            while True:
                print("即将连接redis服务器！")
                cn_img_info = self.redis.spop("cn_img_to_download")
                print("获取到一条信息，开始下载！")
                self.dowmloadImage(cn_img_info)
        except BaseException as e:
            print("run函数抛出异常,异常内容为{}".format(str(e)))

    def dowmloadImage(self, cn_img_info, repeat_count=0):
        src = cn_img_info.split("********")[0]
        alt = cn_img_info.split("********")[1]
        tag = cn_img_info.split("********")[2]
        try:
            start_time = time.time()
            self.count += 1
            img_response = requests.get(src)
            dir_path = "C:/giphy/{}".format(tag)
            self.makeDir(dir_path,"列表文件夹")
            if img_response.status_code == 200 or img_response.status_code == 304:
                file_extension = src.split("/")[-1].split(".")[-1]
                file_name = "{}-{}.{}".format(alt, self.count, file_extension)
                file_path = "{}/{}".format(dir_path, file_name)
                img_content = img_response.content
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName, file_path, end_time - start_time))
                    self.redis.sadd("success_download_img", cn_img_info)
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
                self.redis.sadd("failed_download_img", cn_img_info)

    def makeDir(self, dir_path, type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("成功创建%s%s" % (type, dir_path))
        except BaseException as e:
            print("创建%s异常，错误信息为%s" % (type, str(e)))


def main():
    imgDownloadThreadMount = 50
    block_que = Queue(1)
    block_que.put(11111111111)
    for i in range(imgDownloadThreadMount):
        worker = ImageDownLoader("图片下载线程%d" % (i))
        worker.start()
    block_que.join()


if __name__ == "__main__":
    main()

