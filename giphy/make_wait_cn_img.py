import redis

red=redis.Redis('192.168.1.101', 6379,password="k6i7986t")
red2=redis.Redis('127.0.0.1', 6379)
list=red.smembers("en_img_info2")
count=0
for item in list:
    red2.sadd("en_img_info2",item)
    count+=1
    print(count)
