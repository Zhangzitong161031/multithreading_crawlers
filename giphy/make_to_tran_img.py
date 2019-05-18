import redis

localredis=redis.Redis("127.0.0.1",6379,decode_responses=True)
remoterdis=redis.Redis("192.168.1.101",6379,password="k6i7986t",decode_responses=True)
remote_cn_img_info2s=remoterdis.smembers("cn_img_info2")
for remote_cn_img_info2 in remote_cn_img_info2s:
    localredis.sadd("")



