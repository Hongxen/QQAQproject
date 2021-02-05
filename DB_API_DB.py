#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
from time import strftime
from datetime import datetime, timedelta
import urllib.request
#from apscheduler.schedulers.blocking import BlockingScheduler
import time
import requests
import numpy as np
import pymysql


# In[22]:


r = requests.get("https://data.epa.gov.tw/api/v1/aqx_p_432?limit=1000&api_key=9be7b239-557b-4c10-9775-78cadfc555e9&format=json", verify=False)
a=json.loads(r.text)
#for i in range(len(a['records'])):#搜尋每個測站
label=a['records'][0]   
print(label['SiteName'],label['County'],label['SiteId'],label['Longitude'],label['Latitude'])


# In[23]:


conn = pymysql.Connect(host = '127.0.0.1',
                       port = 3306,
                       user = 'QQAQ',
                       passwd = 'qqaqqqaq',
                       db = 'qqaq',
                       charset='utf8')
H=datetime.now().hour
for i in range(len(a['records'])):#搜尋每個測站
    label=a['records'][i]
    sid=int(label['SiteId'])
    AQI=str(label['AQI'])
    SO2=str(label['SO2'])
    PM25=str(label['PM2.5'])
    PM10=str(label['PM10'])
    CO=str(label['CO'])
    O3=str(label['O3'])
    direct=str(label['WindDirec'])
    speed=str(label['WindSpeed'])
    cur = conn.cursor()
    #sql = "INSERT INTO `station_info`(`id`,`station`, `county`,`longitude`, `latitude`) values(%d,'%s','%s',%f,%f);"%(sid,station,county,longt,laitt)
    #values = (label['SiteId'],label['SiteName'],label['County'],label['Longitude'],label['Latitude'])
    sql = "INSERT INTO `history`(`sid`, `AQI`, `PM10`, `O3`, `CO`, `SO2`, `PM25`, `direct`, `speed`) VALUES( %d,'%s','%s','%s','%s','%s','%s','%s','%s');"%(sid,AQI,PM10,O3,CO,SO2,PM25,direct,speed)
    cur.execute(sql)
    conn.commit()


# In[30]:


Y=int(datetime.now().year)
M=int(datetime.now().month)
D=int(datetime.now().day)
H=int(datetime.now().hour)
county = ['基隆','臺北','板橋','新屋','新竹','新竹','國三S156K','臺中','田中','日月潭','古坑','嘉義','國一N250K','永康','高雄','恆春','宜蘭','花蓮','臺東','澎湖','金門','馬祖']
s = requests.get("https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0003-001?Authorization=CWB-5702404A-5F4B-4CFE-8162-C11C29E66532&format=JSON&elementName=", verify=False)
b=json.loads(s.text)
for i in range(len(b['records']['location'])):
    city=b['records']['location'][i]['parameter'][0]['parameterValue']
    cout=b['records']['location'][i]['locationName']
    temp=b['records']['location'][i]['weatherElement'][3]['elementValue']#溫度
    humid=b['records']['location'][i]['weatherElement'][4]['elementValue']#濕度
    if float(temp) == -99:
        temp = np.nan
    if float(humid) == -99:
        humid = np.nan
    for j in range(len(county)):
        if cout==county[j]:
            if j == 5:
                city = '新竹市'
            elif j == 12:
                city = '嘉義縣'
            #print(j,city,temp,humid)
            temp=str(temp)
            humid=str(humid)
            cur = conn.cursor()
            #sql = "INSERT INTO `station_info`(`id`,`station`, `county`,`longitude`, `latitude`) values(%d,'%s','%s',%f,%f);"%(sid,station,county,longt,laitt)
            #values = (label['SiteId'],label['SiteName'],label['County'],label['Longitude'],label['Latitude'])
            sql = "UPDATE `history` ,`station_info`SET `temp`='%s',`humid`='%s' WHERE history.sid=station_info.id and station_info.county='%s' and EXTRACT(YEAR FROM history.time)=%d and EXTRACT(MONTH FROM history.time)=%d and EXTRACT(DAY FROM history.time)=%d and EXTRACT(HOUR FROM history.time)=%d;"%(temp,humid,city,Y,M,D,H)
            cur.execute(sql)
            conn.commit()
            #weather.append({'county':city, 'humid':humid, 'temp':temp})


# In[34]:


county = ['基隆','臺北','板橋','桃園','新竹市東區','竹東','苗栗','臺中','田中','南投','西螺','嘉義','太保','臺南','高雄','屏東','宜蘭','瑞穗','臺東','澎湖','金門','馬祖']
t = requests.get("https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0002-001?Authorization=CWB-5702404A-5F4B-4CFE-8162-C11C29E66532&format=JSON", verify=False)
#api_key可能須定期更新
c=json.loads(t.text)
for i in range(len(c['records']['location'])):
    label=c['records']['location'][i]['locationName']
    rain=c['records']['location'][i]['weatherElement'][1]['elementValue']#雨量
    city=c['records']['location'][i]['parameter'][0]['parameterValue']#城市
    if float(rain)< 0:
        if float(rain) == -998.0:
            rain = 0
        else:
            rain = np.nan
    else:
        rain = 1
    for j in range(len(county)):
        if label==county[j]:
            #print(j+1,city,rain)
            cur = conn.cursor()
            #sql = "INSERT INTO `station_info`(`id`,`station`, `county`,`longitude`, `latitude`) values(%d,'%s','%s',%f,%f);"%(sid,station,county,longt,laitt)
            #values = (label['SiteId'],label['SiteName'],label['County'],label['Longitude'],label['Latitude'])
            sql = "UPDATE `history` ,`station_info`SET `rain`=%d WHERE history.sid=station_info.id and station_info.county='%s' and EXTRACT(YEAR FROM history.time)=%d and EXTRACT(MONTH FROM history.time)=%d and EXTRACT(DAY FROM history.time)=%d and EXTRACT(HOUR FROM history.time)=%d;"%(rain,city,Y,M,D,H)
            cur.execute(sql)
            conn.commit()

