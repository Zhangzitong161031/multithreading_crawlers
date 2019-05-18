import redis


redi=redis.Redis('127.0.0.1', 6379)
list=redi.smembers("cn_img_to_download")
count=0
for item in list:
    cn_info_arr=item.split("********")
    cn_info_url=cn_info_arr[0]
    count+=1
    print(count)
