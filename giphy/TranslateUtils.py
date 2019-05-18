import requests
import re
import execjs
from requestHeaders import htmlRequestHeaders,ajaxRequestHeaders
import threading
import json
import redis
import time
from StatusCodeError import StatusCodeError

class TranslateUtils(threading.Thread):
    def __init__(self,threadName,validip_que,en_img_info_que,cn_img_info_que):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.daemon = True
        self.en_img_info_que = en_img_info_que
        self.validip_que=validip_que
        self.cn_img_info_que=cn_img_info_que
        self.htmlRequestHeaders=htmlRequestHeaders
        self.ajaxRequestHeaders=ajaxRequestHeaders
        self.htmlUrl="https://fanyi.baidu.com/translate?aldtype=16047&query=&keyfrom=baidu&smartresult=dict&lang=auto2zh"
        self.ajaxUrl="https://fanyi.baidu.com/v2transapi"
        self.from_lan="en"
        self.to_lan="cn"
        self.redis = redis.Redis('127.0.0.1', 6379)

    def run(self,repeat_count=0):
        print("{}开始运行".format(self.threadName))
        while not self.en_img_info_que.empty():
            en_img_info = self.en_img_info_que.get()
            self.startTranslate(en_img_info)
        else:
            print("英文图片信息队列为空，{}即将等待30秒".format(self.name))
            time.sleep(30)
            self.run()

    def startTranslate(self,en_img_info,repeat_count=0):
        en_img_arr = en_img_info.split("********")
        en_alt = en_img_arr[1]
        self.en_word = en_alt
        self.getHtmlCode()
        self.getSign()
        self.getToken()
        if self.sendAjaxRequest():
            cn_img_info = "{}********{}********{}".format(en_img_arr[0], self.cn_word, en_img_arr[2])
            self.cn_img_info_que.put_unique(cn_img_info)
            print("{}成功翻译{}到{}，已翻译的图片信息数量为{},待翻译的图片的数量为{}".format(self.threadName, self.en_word, self.cn_word,self.cn_img_info_que.qsize(),self.en_img_info_que.qsize()))
            time.sleep(1)
            self.redis.sadd("cn_img_info", cn_img_info)
        else:
            self.redis.sadd("failed_tran_img_info",en_img_info )

    def getHtmlCode(self):
        # 获取网页源码
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.get(self.htmlUrl,proxies=proxy,headers=self.htmlRequestHeaders)
            if response.status_code == 200 or response.status_code == 304:
                self.validip_que.put(validip)
                response.encoding = 'utf-8'
                self.html=response
            else:
                raise StatusCodeError("翻译模块获取htmlcode状态码错误，错误状态码为".format(response.status_code))
        except Exception as e:
            print("获取html出现异常，异常内容为{}".format(str(e)))

    def getSign(self):
        try:
            matches = re.findall("window.gtk = '(.*?)';", self.html.text, re.S)
            for match in matches:
                gtk = match
            if gtk == "":
                print('Get gtk fail.')
                exit()
            # 计算 sign
            signJsCode = '''
                function n(r, o) {
                    for (var t = 0; t < o.length - 2; t += 3) {
                        var a = o.charAt(t + 2);
                        a = a >= "a"? a.charCodeAt(0) - 87 : Number(a),
                        a = "+" === o.charAt(t + 1) ? r >>> a: r << a,
                        r = "+" === o.charAt(t) ? r + a & 4294967295 : r ^ a
                    }
                    return r
                }
                var i = null;
                function e(r) {
                    var o = r.match(/[\uD800-\uDBFF][\uDC00-\uDFFF]/g);
                    if (null === o) {
                        var t = r.length;
                        t > 30 && (r = "" + r.substr(0, 10) + r.substr(Math.floor(t / 2) - 5, 10) + r.substr( - 10, 10))
                    } else {
                        for (var e = r.split(/[\uD800-\uDBFF][\uDC00-\uDFFF]/), C = 0, h = e.length, f = []; h > C; C++)"" !== e[C] && f.push.apply(f, a(e[C].split(""))),
                        C !== h - 1 && f.push(o[C]);
                        var g = f.length;
                        g > 30 && (r = f.slice(0, 10).join("") + f.slice(Math.floor(g / 2) - 5, Math.floor(g / 2) + 5).join("") + f.slice( - 10).join(""))
                    }
                    var u = void 0, l = "" + String.fromCharCode(103) + String.fromCharCode(116) + String.fromCharCode(107);
                    u = null !== i ? i : (i = '320305.131321201' || "") || "";
                    for (var d = u.split("."), m = Number(d[0]) || 0, s = Number(d[1]) || 0, S = [], c = 0, v = 0; v < r.length; v++) {
                        var A = r.charCodeAt(v);
                        128 > A ? S[c++] = A: (2048 > A ? S[c++] = A >> 6 | 192 : (55296 === (64512 & A) && v + 1 < r.length && 56320 === (64512 & r.charCodeAt(v + 1)) ? (A = 65536 + ((1023 & A) << 10) + (1023 & r.charCodeAt(++v)), S[c++] = A >> 18 | 240, S[c++] = A >> 12 & 63 | 128) : S[c++] = A >> 12 | 224, S[c++] = A >> 6 & 63 | 128), S[c++] = 63 & A | 128)
                    }
                    for (var p = m,
                    F = "" + String.fromCharCode(43) + String.fromCharCode(45) + String.fromCharCode(97) + ("" + String.fromCharCode(94) + String.fromCharCode(43) + String.fromCharCode(54)), D = "" + String.fromCharCode(43) + String.fromCharCode(45) + String.fromCharCode(51) + ("" + String.fromCharCode(94) + String.fromCharCode(43) + String.fromCharCode(98)) + ("" + String.fromCharCode(43) + String.fromCharCode(45) + String.fromCharCode(102)), b = 0; b < S.length; b++) p += S[b],
                    p = n(p, F);
                    return p = n(p, D),
                    p ^= s,
                    0 > p && (p = (2147483647 & p) + 2147483648),
                    p %= 1e6,
                    p.toString() + "." + (p ^ m)
                }
            '''
            signJsCode.replace("320305.131321201", gtk)
            self.sign = execjs.compile(signJsCode).call('e', self.en_word)
            #print('sign = ' + self.sign)
        except Exception as e:
            print("获取sign出现异常，异常内容为{}".format(str(e)))

    def getToken(self):
        try:
            # 正则匹配 token
            matches = re.findall("token: '(.*?)'", self.html.text, re.S)
            for match in matches:
                self.token = match
            if self.token == "":
                #print('Get token fail.')
                exit()
            #print('token = ' + self.token)
        except Exception as e:
            print("获取token出现异常，异常内容为{}".format(str(e)))


    def sendAjaxRequest(self,repeat_count=0):
        self.param = {
            "from": 'en',
            "to": 'zh',
            "query": self.en_word,
            "transtype": "translang",
            "simple_means_flag": "3",
            "sign": self.sign,
            "token": self.token
        }
        validip = self.validip_que.get()
        proxy = {'http': validip}
        try:
            response = requests.post(self.ajaxUrl,proxies=proxy, headers=self.ajaxRequestHeaders, data=self.param)
            if response.status_code==200 or response.status_code==304:
                self.validip_que.put(validip)
                self.data=response.text
                dict = json.loads(self.data)
                self.cn_word=dict['trans_result']['data'][0]['dst']
                return True
            else:
                raise StatusCodeError("翻译模块sendAjaxRequest状态码错误，错误状态码为".format(response.status_code))
        except Exception as e:
            repeat_count += 1
            if repeat_count <3:
               if "ACCESS LIMIT" in self.data:
                   print("出现ACCESS LIMIT，暂停20秒")
                   time.sleep(20)
                   self.sendAjaxRequest(repeat_count)
               else:
                   print("翻译异常，异常内容为{}正在进行第{}次翻译，data为{}".format(str(e), repeat_count, str(self.data)))
                   self.sendAjaxRequest(repeat_count)
            else:
                print("翻译异常次数超过上限，即将添加到翻译失败数据表,data为".format(self.data))
                return False

def main():
    try:
        translateUtils=TranslateUtils("night")
        translateUtils.run()
    except BaseException as e:
        print("返回数据出现异常,异常内容为{}".format(str(e)))

if __name__ == "__main__":
    main()