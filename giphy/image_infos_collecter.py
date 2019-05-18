# -*- coding:utf-8 -*-
from queue import Queue
import threading
from tag_list import tag_list1
import requests
import json
from StatusCodeError import StatusCodeError
import redis
import time

class ImageInfoCollecter(threading.Thread):
    def __init__(self, threadName,validip_que,tag_que,img_src_check_que,en_img_info_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName=threadName
        self.validip_que=validip_que
        self.img_src_check_que=img_src_check_que
        self.en_img_info_que = en_img_info_que
        self.tag_que=tag_que
        self.failed_count = 0
        self.redis = redis.Redis('127.0.0.1', 6379)

    def run(self):
        print("%s开始启动" % (self.threadName))
        while not self.tag_que.empty():
            tag = self.tag_que.get()
            self.failed_count=0
            tag_api_urls=["https://api.giphy.com/v1/gifs/search?api_key=3eFQvabDx69SMoOemSPiYfh9FY0nzO9x&q={}&offset={}&limit=100".format(tag,(index-1)*25) for index in range(1,201)]
            for tag_api_url in tag_api_urls:
                self.getApiText(tag_api_url,tag)
                if self.failed_count >700:
                    print("{}累计重复次数超过600次，即将跳出循环".format(tag_api_url))
                    break
                time.sleep(10)
            self.redis.sadd("complete_tag",tag)
            self.tag_que.task_done()

    def getApiText(self,url,tag,repeat_count=0):
        try:
            response = requests.get(url,timeout=50)
            if response.status_code == 200:
                json_data=response.text
                data=json.loads(json_data)
                image_arr=data["data"]
                print("成功获取{}个image对象".format(len(image_arr)))
                num=0
                failed_num=0
                for image in image_arr:
                    src=image["images"]["downsized"]["url"]
                    existed=self.img_src_check_que.put_unique(src)
                    self.redis.sadd("img_src_check",src)
                    if existed==0:
                        title=image["title"].split("GIF")[0]
                        en_img_info="{}********{}********{}".format(src,title,tag)
                        if self.en_img_info_que.put_unique(en_img_info)==0:
                            self.redis.sadd("en_img_info",en_img_info)
                            num+=1
                            # print("成功插入一条记录{}，当前队列长度为{}，{}累计重复次数{}".format(en_img_info,self.en_img_info_que.qsize(),tag,self.failed_count))
                        else:
                            failed_num+=1
                            print("图片地址检测程序出现异常")
                    else:
                        failed_num+=1
                        self.failed_count += 1
                        print("插入失败，记录{}已存在！累计失败次数为{}".format(src,self.failed_count))
                print("{}成功插入{}条图片到英文队列，插入失败条数{},当前英文队列总量{},{}类目累计重复次数为{}".format(self.threadName,num,failed_num,self.en_img_info_que.qsize(),tag,self.failed_count))
            else:
                raise StatusCodeError("状态码错误，错误状态码为{}".format(response.status_code))
        except Exception as e:
            print("{}api接口{}请求异常，错误信息为{}".format(self.threadName,url,str(e)))
            repeat_count += 1
            if repeat_count < 3:
                print("api接口{}请求失败，正在进行第{}次重新下载!" .format(url, repeat_count))
                self.getApiText(url, tag,repeat_count)
            else:
                print("api接口{}请求失败，添加至数据库".format(url))
                self.redis.sadd("failed_api_url", url)

def main():
     tag_que=Queue(500000)
     for tag in tag_list:
         tag_que.put(tag)
     img_src_check_que= Queue(7000000)
     en_img_info_que = Queue(7000000)
     dataCollectThreadMount=1
     for i in range(dataCollectThreadMount):
         worker = ImageInfoCollecter("图片地址采集线程%d" % (i),tag_que,img_src_check_que,en_img_info_que)
         worker.start()
     tag_que.join()

if __name__=="__main__":
    tag_list = tag_list1
    main()
