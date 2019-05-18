# -*- coding:gb2312 -*-
import requests
import threading
from queue import Queue
import time
from proxy_helper import Proxy_helper
from bs4 import BeautifulSoup
import os
import pymysql
from mysqlConfig import MysqlConfig
from StatusCodeError import StatusCodeError


class Spider(threading.Thread):
    def __init__(self, threadName, url_queue, validip_que):
        threading.Thread.__init__(self)
        self.daemon = True
        self.threadName = threadName
        self.mysqlConfig = MysqlConfig
        self.url_queue = url_queue
        self.validip_que = validip_que

    def run(self):
        print("%s开始启动" % (self.name))
        self.connectMysql()
        global mysqlInitialized
        global mysqlInitializeLock
        mysqlInitializeLock.acquire()
        if not mysqlInitialized:
            self.initializeMysql()
            mysqlInitialized = True
        mysqlInitializeLock.release()
        self.makeDir("C:/QQ", "下载根目录")
        self.makeDir("C:/QQ/qq", "下载根目录")
        while not self.url_queue.empty():
            url = self.url_queue.get()
            self.getArcHtml(url)
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

    def getListHtml(self, url, repeat_count=0):
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(url, proxies=proxy, timeout=5)
            if response.status_code == 200:
                self.validip_que.put(validip)
                response.encoding = "gb2312"
                soup = BeautifulSoup(response.text, "lxml")
                a_list = soup.select(".txt")
                for a in a_list:
                    arc_url = a.get("href") if "qq.qqjia.com" in a.get("href") else "http://www.qqjia.com/" + a.get(
                        "href")
                    print(arc_url)
                    # self.getArcHtml(arc_url)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("{}请求列表页异常，错误信息为{},行号为{}".format(self.threadName, str(e), e.__traceback__.tb_lineno))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s列表页%s下载失败，正在进行第%d次重新下载!" % (self.threadName, url, repeat_count))
                self.getListHtml(url, repeat_count)
            else:
                print("%s列表页%s下载失败，添加至数据库" % (self.threadName, url))
                self.sqlInsertFailedUrl(url, "list")

    def getArcHtml(self, arc_url, repeat_count=0):
        start_time = time.time()
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(arc_url, proxies=proxy, timeout=5)
            if response.status_code == 200:
                self.validip_que.put(validip)
                response.encoding = "gb2312"
                soup = BeautifulSoup(response.text, "lxml")
                img_list = soup.select('.content_word img')
                title = soup.select('h3')[0].text
                invalid_str_arr = ["/", ".", "\\", "\r\n", "。", "*", '"', "<", ">", "|", "?", "？", ":"]
                for invalid_str in invalid_str_arr:
                    title = title.replace(invalid_str, "")
                index = 0
                for img in img_list:
                    index += 1
                    src = img.get("src")
                    alt = img.get("alt")
                    file_extension_name = src.split("!")[0].split("/")[-1].split(".")[-1]
                    file_name = "{}-{}.{}".format(title, index, file_extension_name)
                    file_path = "C:/QQ/qq" + "/" + file_name
                    self.downloadImage(src, file_path)
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("%s详情页下载异常，错误信息为%s" % (self.threadName, str(e)))
            repeat_count += 1
            if repeat_count < 4:
                print("%s详情页%s下载失败，正在进行第%d次重新下载!" % (self.threadName, arc_url, repeat_count))
                self.getArcHtml(arc_url, repeat_count)
            else:
                print("%s详情页%s下载失败" % (self.threadName, arc_url))
                self.sqlInsertFailedUrl(arc_url, "article")

    def downloadImage(self, src, file_path, repeat_count=0):
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            start_time = time.time()
            response = requests.get(src, proxies=proxy)
            if response.status_code == 200:
                img_content = response.content
                with open(file_path, "wb") as f:
                    f.write(img_content)
                    end_time = time.time()
                    inter = end_time - start_time
                    print("%s成功下载图片%s，共花费%f秒" % (self.threadName, file_path, inter))
            else:
                raise StatusCodeError("状态码错误")
        except BaseException as e:
            print("%s图片下载异常，错误信息为%s" % (self.threadName, str(e)))
            self.validip_que.get(validip)
            repeat_count += 1
            if repeat_count < 4:
                print("%s图片%s下载抛出异常，正在进行第%d次重新下载!" % (self.threadName, src, repeat_count))
                self.downloadImage(src, file_path, repeat_count)
            else:
                print("%s图片%s下载失败，将添加下载失败信息到数据表" % (self.threadName, src))
                self.sqlInsertFailedUrl(src, "failed_image")

    def makeDir(self, dir_path, type):
        try:
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
                print("%s成功创建%s%s" % (self.threadName, type, dir_path))
        except BaseException as e:
            print("%s创建%s异常，错误信息为%s" % (self.threadName, type, str(e)))

    def sqlInsertFailedUrl(self, url, type):
        try:
            global sql
            sql = """INSERT IGNORE INTO qq_failed_{}_url(url) VALUES ('{}')""".format(type, url)
            if self.mysqlClient.cursor().execute(sql):
                self.mysqlClient.commit()
                print("{}成功插入一条错误的{}记录到数据库".format(self.threadName, type))
        except BaseException as e:
            print("{}sqlInsertFailedUrl抛出异常，异常内容为:{}".format(self.threadName, str(e)))


def main():
    # 开启多线程采集代理IP，并放置于代理IP的队列ipproxy_que里
    ip_que = Queue(1200)
    validip_que = Queue(10000)
    ipCheckoutThreadMount = 40
    ipCollectThreadMount = 2
    dataCollectThreadMount = 100
    proxy_helper = Proxy_helper(ip_que, validip_que, ipCheckoutThreadMount, ipCollectThreadMount)
    proxy_helper.run()
    time.sleep(20)
    # url_list = ["      "http://www.qqjia.com/biaoqing/index%d.htm","%(index) for index in range(2,14)]
    url_list = [
        "http://www.qqjia.com//htm/hao24966.htm",
        "http://www.qqjia.com//htm/hao24965.htm",
        "http://www.qqjia.com//htm/hao24964.htm",
        "http://www.qqjia.com//htm/hao24847.htm",
        "http://www.qqjia.com//htm/hao24846.htm",
        "http://www.qqjia.com//htm/hao24845.htm",
        "http://www.qqjia.com//htm/hao24798.htm",
        "http://www.qqjia.com//learn/qiubilong.htm",
        "http://qq.qqjia.com/bq/ali.htm",
        "http://qq.qqjia.com/bq/dingdang.htm",
        "http://qq.qqjia.com/bq/zhaocai.htm",
        "http://qq.qqjia.com/bq/dubao.htm",
        "http://www.qqjia.com//learn/fayuebing.htm",
        "http://www.qqjia.com//learn/bqjr41.htm",
        "http://qq.qqjia.com/bq/xiaoyaoji.htm",
        "http://qq.qqjia.com/bq/egao.htm",
        "http://www.qqjia.com//learn/bqjr101.htm",
        "http://www.qqjia.com//learn/bqjr1225.htm",
        "http://qq.qqjia.com/bq/christmas2.htm",
        "http://www.qqjia.com//learn/HMstyle.htm",
        "http://www.qqjia.com//learn/bqjr214.htm",
        "http://qq.qqjia.com/bq/yuanfang.htm",
        "http://qq.qqjia.com/bq/aduo.htm",
        "http://qq.qqjia.com/bq/christmas.htm",
        "http://qq.qqjia.com/bq/jr1111b.htm",
        "http://qq.qqjia.com/bq/wodi.htm",
        "http://qq.qqjia.com/bq/riben.htm",
        "http://qq.qqjia.com/bq/xxhu.htm",
        "http://www.qqjia.com//learn/bqjr1001.htm",
        "http://qq.qqjia.com/bq/baozou.htm",
        "http://qq.qqjia.com/bq/guoqing.htm",
        "http://qq.qqjia.com/bq/jr910b.htm",
        "http://qq.qqjia.com/bq/zhongqiu.htm",
        "http://qq.qqjia.com/bq/jr77b.htm",
        "http://www.qqjia.com//learn/bqjr910.htm",
        "http://qq.qqjia.com/bq/wabishi.htm",
        "http://www.qqjia.com//learn/bq28.htm",
        "http://www.qqjia.com//learn/bq26.htm",
        "http://www.qqjia.com//learn/bq21.htm",
        "http://www.qqjia.com//learn/bq20.htm",
        "http://www.qqjia.com//learn/bq10.htm",
        "http://www.qqjia.com//learn/bq06.htm",
        "http://www.qqjia.com//learn/bq09.htm",
        "http://www.qqjia.com//learn/bq03.htm",
        "http://qq.qqjia.com/bq/mogui.htm",
        "http://qq.qqjia.com/bq/jr1031b.htm",
        "http://qq.qqjia.com/bq/ge.htm",
        "http://qq.qqjia.com/bq/xigua.htm",
        "http://qq.qqjia.com/bq/yu.htm",
        "http://qq.qqjia.com/bq/pitou.htm",
        "http://qq.qqjia.com/bq/danwen.htm",
        "http://qq.qqjia.com/bq/danhuang.htm",
        "http://qq.qqjia.com/bq/wulagui.htm",
        "http://qq.qqjia.com/bq/Sinbo.htm",
        "http://qq.qqjia.com/bq/ruiqie.htm",
        "http://qq.qqjia.com/bq/miqi.htm",
        "http://qq.qqjia.com/bq/chaoren.htm",
        "http://qq.qqjia.com/bq/spl.htm",
        "http://qq.qqjia.com/bq/zgl.htm",
        "http://qq.qqjia.com/bq/612.htm",
        "http://qq.qqjia.com/bq/xyq.htm",
        "http://qq.qqjia.com/bq/tuzibang.htm",
        "http://www.qqjia.com//learn/bq27.htm",
        "http://qq.qqjia.com/bq/Copycat.htm",
        "http://qq.qqjia.com/bq/sha.htm",
        "http://qq.qqjia.com/bq/habao.htm",
        "http://qq.qqjia.com/bq/qqnc.htm",
        "http://qq.qqjia.com/bq/qunzhu.htm",
        "http://qq.qqjia.com/bq/paobin3.htm",
        "http://qq.qqjia.com/bq/paobin2.htm",
        "http://qq.qqjia.com/bq/paobin1.htm",
        "http://qq.qqjia.com/bq/pangxie.htm",
        "http://qq.qqjia.com/bq/shengri.htm",
        "http://qq.qqjia.com/bq/xingmao.htm",
        "http://qq.qqjia.com/bq/shua.htm",
        "http://qq.qqjia.com/bq/yoci2.htm",
        "http://qq.qqjia.com/bq/yoci.htm",
        "http://qq.qqjia.com/bq/zhuai.htm",
        "http://qq.qqjia.com/bq/bbgou.htm",
        "http://qq.qqjia.com/bq/ziziji.htm",
        "http://qq.qqjia.com/bq/tusiji.htm",
        "http://qq.qqjia.com/bq/aoyun.htm",
        "http://qq.qqjia.com/bq/qq2007.htm",
        "http://qq.qqjia.com/bq/bijia.htm",
        "http://qq.qqjia.com/bq/bianxing.htm",
        "http://qq.qqjia.com/bq/mogutou.htm",
        "http://qq.qqjia.com/bq/zhumm.htm",
        "http://qq.qqjia.com/bq/benko.htm",
        "http://qq.qqjia.com/bq/hundun.htm",
        "http://qq.qqjia.com/bq/PUCCA.htm",
        "http://qq.qqjia.com/bq/gxylk.htm",
        "http://qq.qqjia.com/bq/yongbao.htm",
        "http://qq.qqjia.com/bq/iPadQQ.htm",
        "http://qq.qqjia.com/bq/bainian.htm",
        "http://qq.qqjia.com/bq/jr214b.htm",
        "http://qq.qqjia.com/bq/keaidw.htm",
        "http://qq.qqjia.com/bq/keaidw2.htm",
        "http://qq.qqjia.com/bq/kjqd.htm",
        "http://qq.qqjia.com/bq/zhongqiu2.htm",
        "http://qq.qqjia.com/bq/xiyangyang.htm",
        "http://qq.qqjia.com/bq/qiu.htm",
        "http://www.qqjia.com//learn/bqjr77.htm",
        "http://qq.qqjia.com/bq/xcb.htm",
        "http://qq.qqjia.com/bq/jr41b.htm",
        "http://qq.qqjia.com/bq/labi.htm",
        "http://qq.qqjia.com/bq/chaye.htm",
        "http://qq.qqjia.com/bq/jr38b.htm",
        "http://qq.qqjia.com/bq/chunjie.htm",
        "http://qq.qqjia.com/bq/xcj.htm",
        "http://qq.qqjia.com/bq/ytmm.htm",
        "http://qq.qqjia.com/bq/jiaozi.htm",
        "http://qq.qqjia.com/bq/yutumei.htm",
        "http://qq.qqjia.com/bq/fangkuai.htm",
        "http://qq.qqjia.com/bq/momo.htm",
        "http://qq.qqjia.com/bq/haidi.htm",
        "http://qq.qqjia.com/bq/duyan.htm",
        "http://www.qqjia.com//htm/hao33742.htm",
        "http://www.qqjia.com//learn/bqjr11.htm",
        "http://qq.qqjia.com/bq/jr11b.htm",
        "http://qq.qqjia.com/bq/yuandan.htm",
        "http://www.qqjia.com//learn/bqjr815.htm",
        "http://www.qqjia.com//htm/hao29095.htm",
        "http://www.qqjia.com//htm/hao29013.htm",
        "http://www.qqjia.com//htm/hao29012.htm",
        "http://www.qqjia.com//htm/hao27510.htm",
        "http://www.qqjia.com//htm/hao27509.htm",
        "http://www.qqjia.com//htm/hao26984.htm",
        "http://www.qqjia.com//htm/hao26983.htm",
        "http://www.qqjia.com//htm/hao25819.htm",
        "http://www.qqjia.com//htm/hao25817.htm",
        "http://www.qqjia.com//htm/hao25816.htm",
        "http://www.qqjia.com//htm/hao25725.htm",
        "http://www.qqjia.com//htm/hao25724.htm",
        "http://www.qqjia.com//htm/hao25675.htm",
        "http://www.qqjia.com//htm/hao25674.htm",
        "http://www.qqjia.com//htm/hao25673.htm",
        "http://www.qqjia.com//htm/hao25672.htm",
        "http://www.qqjia.com//htm/hao25568.htm",
        "http://www.qqjia.com//htm/hao25567.htm",
        "http://www.qqjia.com//htm/hao25425.htm",
        "http://qq.qqjia.com/bq/zhenggu.htm",
        "http://qq.qqjia.com/bq/kongbu.htm",
        "http://qq.qqjia.com/bq/yeman.htm",
        "http://qq.qqjia.com/bq/qqqun.htm",
        "http://qq.qqjia.com/bq/pohai.htm",
        "http://qq.qqjia.com/bq/zaobs.htm",
        "http://qq.qqjia.com/bq/dianhou.htm",
        "http://qq.qqjia.com/bq/hongyt.htm",
        "http://qq.qqjia.com/bq/milaoshu.htm",
        "http://qq.qqjia.com/bq/jr11zhu.htm",
        "http://qq.qqjia.com/bq/kitten.htm",
        "http://qq.qqjia.com/bq/gxbq.htm",
        "http://qq.qqjia.com/bq/petzhu.htm",
        "http://qq.qqjia.com/bq/2006logo.htm",
        "http://qq.qqjia.com/bq/youxi.htm",
        "http://qq.qqjia.com/bq/leonc.htm",
        "http://qq.qqjia.com/bq/leonb.htm",
        "http://qq.qqjia.com/bq/leona.htm",
        "http://qq.qqjia.com/bq/tudou.htm",
        "http://qq.qqjia.com/bq/Dori.htm",
        "http://qq.qqjia.com/bq/xihahou.htm",
        "http://qq.qqjia.com/bq/keaikonlon.htm",
        "http://qq.qqjia.com/bq/xiaoqiang.htm",
        "http://qq.qqjia.com/bq/keaihg.htm",
        "http://qq.qqjia.com/bq/baoer.htm",
        "http://qq.qqjia.com/bq/xiongmao.htm",
        "http://qq.qqjia.com/bq/xiaojiji.htm",
        "http://qq.qqjia.com/bq/shoushi.htm",
        "http://qq.qqjia.com/bq/Mocmoc.htm",
        "http://qq.qqjia.com/bq/caizhong.htm",
        "http://qq.qqjia.com/bq/caiabc.htm",
        "http://qq.qqjia.com/bq/kaka.htm",
        "http://qq.qqjia.com/bq/huangdi.htm",
        "http://qq.qqjia.com/bq/box.htm",
        "http://qq.qqjia.com/bq/huoju.htm",
        "http://qq.qqjia.com/bq/jxry.htm",
        "http://qq.qqjia.com/bq/hellocai.htm",
        "http://qq.qqjia.com/bq/gupiao.htm",
        "http://qq.qqjia.com/bq/upup.htm",
        "http://qq.qqjia.com/bq/xiaohuimao.htm",
        "http://qq.qqjia.com/bq/naitouzai.htm",
        "http://qq.qqjia.com/bq/lvtoujin.htm",
        "http://qq.qqjia.com/bq/yangcong.htm",
        "http://qq.qqjia.com/bq/moguai.htm",
        "http://qq.qqjia.com/bq/caicai.htm",
        "http://qq.qqjia.com/bq/quhou.htm",
        "http://qq.qqjia.com/bq/qumao.htm",
        "http://qq.qqjia.com/bq/qutu.htm",
        "http://www.qqjia.com//learn/bqjr520.htm",
        "http://www.qqjia.com//learn/bq12.htm",
        "http://www.qqjia.com//learn/bq05.htm",
        "http://www.qqjia.com//learn/bq13.htm",
        "http://www.qqjia.com//learn/bq14.htm",
        "http://www.qqjia.com//learn/bq15.htm",
        "http://www.qqjia.com//learn/bq04.htm",
        "http://qq.qqjia.com/bq/gongxi.htm",
        "http://www.qqjia.com//learn/bq17.htm",
        "http://www.qqjia.com//learn/bq18.htm",
        "http://www.qqjia.com//learn/bq19.htm",
        "http://www.qqjia.com//learn/bq23.htm",
        "http://www.qqjia.com//learn/bq22.htm",
        "http://www.qqjia.com//learn/bq25.htm",
        "http://www.qqjia.com//learn/bqsr.htm",
        "http://www.qqjia.com//learn/bq24.htm",
        "http://www.qqjia.com//learn/bqmms1.htm",
        "http://www.qqjia.com//learn/bqmms2.htm",
        "http://www.qqjia.com//learn/bqmms3.htm",
        "http://www.qqjia.com//learn/bqmms4.htm",
        "http://www.qqjia.com//learn/bqmms5.htm",
        "http://www.qqjia.com//learn/bqmms6.htm",
        "http://www.qqjia.com//learn/bqmms7.htm",
        "http://www.qqjia.com//learn/bqmms8.htm",
        "http://qq.qqjia.com/bq/qingwa.htm",
        "http://qq.qqjia.com/bq/zhutou.htm",
        "http://qq.qqjia.com/bq/heibaizhu.htm",
        "http://qq.qqjia.com/bq/huahua.htm",
        "http://qq.qqjia.com/bq/chaonv.htm",
        "http://qq.qqjia.com/bq/chaonv2.htm",
        "http://qq.qqjia.com/bq/aishui.htm",
        "http://qq.qqjia.com/bq/coolfeng3.htm",
        "http://qq.qqjia.com/bq/coolfeng2.htm",
        "http://qq.qqjia.com/bq/xiaohai.htm",
        "http://qq.qqjia.com/bq/set3.htm",
        "http://qq.qqjia.com/bq/cywenzi.htm",
        "http://qq.qqjia.com/bq/jdrenwu.htm",
        "http://qq.qqjia.com/bq/niuniu.htm",
        "http://qq.qqjia.com/bq/shm.htm",
        "http://qq.qqjia.com/bq/datou.htm",
        "http://qq.qqjia.com/bq/baozi.htm",
        "http://qq.qqjia.com/bq/yctdtt.htm",
        "http://qq.qqjia.com/bq/keainv.htm",
        "http://qq.qqjia.com/bq/tqqq.htm",
        "http://qq.qqjia.com/bq/tqqq2.htm",
        "http://qq.qqjia.com/bq/xingfa.htm",
        "http://qq.qqjia.com/bq/suihai.htm",
        "http://qq.qqjia.com/bq/sanmao.htm",
        "http://qq.qqjia.com/bq/bao.htm",
        "http://qq.qqjia.com/bq/kittenxin.htm",
        "http://qq.qqjia.com/bq/kittenbaozi.htm",
        "http://qq.qqjia.com/bq/kittenyuqi.htm",
        "http://qq.qqjia.com/bq/wanwan.htm",
        "http://www.qqjia.com//learn/bq16.htm",
        "http://qq.qqjia.com/bq/fuqin.htm",
        "http://qq.qqjia.com/bq/boto.htm",
        "http://qq.qqjia.com/bq/jr61b.htm",
        "http://qq.qqjia.com/bq/yinshi.htm",
        "http://qq.qqjia.com/bq/kittenzimu.htm",
        "http://qq.qqjia.com/bq/gxcx.htm",
        "http://qq.qqjia.com/bq/xiaoqq.htm",
        "http://qq.qqjia.com/bq/maomi.htm",
        "http://qq.qqjia.com/bq/rmb.htm",
        "http://qq.qqjia.com/bq/jss.htm",
        "http://qq.qqjia.com/bq/zxw.htm",
        "http://qq.qqjia.com/bq/xiaoji.htm",
        "http://qq.qqjia.com/bq/zhenzi.htm",
        "http://www.qqjia.com//learn/bqjr51.htm",
        "http://qq.qqjia.com/bq/jr51b.htm",
        "http://qq.qqjia.com/bq/konglong.htm",
        "http://qq.qqjia.com/bq/guoqing2.htm",
        "http://qq.qqjia.com/bq/ktmb.htm",
        "http://www.qqjia.com//learn/bqjr61.htm",
        "http://qq.qqjia.com/bq/qggqmm.htm",
        "http://www.qqjia.com//learn/laobq.htm",
        "http://qq.qqjia.com/bq/wsxiong.htm",
        "http://www.qqjia.com//learn/bqjr38.htm",
        "http://qq.qqjia.com/bq/erka.htm",
        "http://qq.qqjia.com/bq/jr520b.htm",
        "http://qq.qqjia.com/bq/jieji.htm",
        "http://qq.qqjia.com/bq/jr55b.htm",
        "http://qq.qqjia.com/bq/tuzi.htm",
        "http://www.qqjia.com//learn/bqjr620.htm",
        "http://qq.qqjia.com/bq/shengxiao.htm",
        "http://www.qqjia.com//learn/bqjr55.htm",
        "http://qq.qqjia.com/bq/youzi.htm",
        "http://www.qqjia.com//learn/bq01.htm",
        "http://qq.qqjia.com/bq/qqpet.htm",
        "http://www.qqjia.com//learn/bq02.htm",
        "http://www.qqjia.com//learn/bqjr115.htm",
        "http://www.qqjia.com//learn/bq07.htm",
        "http://www.qqjia.com//learn/bq08.htm",
        "http://www.qqjia.com//learn/bq11.htm",
        "http://qq.qqjia.com/bq/jr115b.htm",
        "http://qq.qqjia.com/bq/wsm.htm",
        "http://qq.qqjia.com/bq/haha.htm",
        "http://qq.qqjia.com/bq/aoao.htm",
        "http://qq.qqjia.com/bq/pb.htm",
        "http://qq.qqjia.com/bq/moumouniu.htm",
        "http://qq.qqjia.com/bq/woniu.htm",
        "http://qq.qqjia.com/bq/zhuzhuxia.htm",
        "http://qq.qqjia.com/bq/maomaoshu.htm",
        "http://qq.qqjia.com/bq/maomaoshu3D.htm",
        "http://qq.qqjia.com/bq/xiaobai.htm",
        "http://qq.qqjia.com/bq/kaka2.htm",
        "http://qq.qqjia.com/bq/diandian.htm",
        "http://qq.qqjia.com/bq/txb.htm",
        "http://qq.qqjia.com/bq/xsy.htm",
        "http://qq.qqjia.com/bq/xiaoK.htm",
        "http://qq.qqjia.com/bq/SuperPower.htm",
        "http://qq.qqjia.com/bq/wanggou.htm",
        "http://qq.qqjia.com/bq/zfg.htm",
        "http://qq.qqjia.com/bq/qbaycat.htm",
        "http://qq.qqjia.com/bq/youa.htm",
        "http://qq.qqjia.com/bq/jiujiu.htm",
        "http://qq.qqjia.com/bq/qun.htm",
        "http://qq.qqjia.com/bq/zheng.htm",
        "http://qq.qqjia.com/bq/yiwai.htm",
        "http://www.qqjia.com//htm/hao25424.htm",
        "http://www.qqjia.com//htm/hao25423.htm",
        "http://www.qqjia.com//htm/hao25422.htm",
        "http://www.qqjia.com//htm/hao24967.htm",






    ]
    url_que = Queue(1000)
    # url_que.put("      "http://www.qqjia.com/biaoqing/")
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
