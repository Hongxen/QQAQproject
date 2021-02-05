import json
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
from time import strftime
from datetime import datetime, timedelta
import urllib.request
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import requests
import numpy as np

#Catch the temperature and humidity data of counties and cities
def crawl_weather():
    weather = []
    countyID = ['466940','466920','466880','467050','467571','CAE090','467490','467270','467650','A2K570','467480','CAL010','467420','467440','467590','467080','466990','467660','467350','467110','467990']
    id = ['31','66','46','34','53','22','36','70','13','40','63','14','56','18','12','38','50','44','1','33','43','32']
    #id = ['1','12','6','17','23','24','26','30','33','36','37','42','40','43','50','61','65','63','62','78','77','75']
    r = requests.get("https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0003-001?Authorization=CWB-5702404A-5F4B-4CFE-8162-C11C29E66532&format=JSON&elementName=")
    a=json.loads(r.text)
    Y=datetime.now().year
    M=datetime.now().month
    D=datetime.now().day
    H=datetime.now().hour
    for i in range(len(a['records']['location'])):
        city=a['records']['location'][i]['parameter'][0]['parameterValue']
        station = a['records']['location'][i]['locationName']
        label=a['records']['location'][i]['stationId']
        temp=a['records']['location'][i]['weatherElement'][3]['elementValue']#temperature
        humid=a['records']['location'][i]['weatherElement'][4]['elementValue']#humidity
        if float(temp) == -99:
            temp = np.nan
        if float(humid) == -99:
            humid = np.nan
        for j in range(len(countyID)):
          if label==countyID[j]:
            #print(j,city,temp,humid)
            # weather.append({'county':city, 'humid':humid, 'temp':temp})
            weather.append({'date': {'year': int(Y), 'month': int(M), 'day': int(D)}, 'station': station, 'feature': 'humid', 'id': id[j], 'hr_data': [{'hr_v':humid, 'hr_t': int(H), 'hr_flag': 0}], 
                      'county':city})
            weather.append({'date': {'year': int(Y), 'month': int(M), 'day': int(D)}, 'station': station, 'feature': 'temp', 'id': id[j], 'hr_data': [{'hr_v':temp, 'hr_t': int(H), 'hr_flag': 0}], 
                      'county':city})            
    return weather

#Check whether it is raining in counties or cities    
def crawl_Rain():
    rainL = []
    countyID = ['466940','466920','466880','C0C480','C0D660','C0D560','C0E750','467490','467270','C0I460','C0K420','467480','C0M680','467410','467440','C0R170','467080','C0Z210','467660','467350','467110','467990']
    id = ['31','66','46','34','53','22','36','70','13','40','63','14','56','18','12','38','50','44','1','33','43','32']
    r = requests.get("https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0002-001?Authorization=CWB-5702404A-5F4B-4CFE-8162-C11C29E66532&format=JSON", verify=False)
    #api_key may need to be updated regularly
    a=json.loads(r.text)
    Y=datetime.now().year
    M=datetime.now().month
    D=datetime.now().day
    H=datetime.now().hour
    for i in range(len(a['records']['location'])):
        station = a['records']['location'][i]['locationName']
        label=a['records']['location'][i]['stationId']
        rain=a['records']['location'][i]['weatherElement'][1]['elementValue']#raining
        city=a['records']['location'][i]['parameter'][0]['parameterValue']#cities
        if float(rain)< 0:
            if float(rain) == -998.0:
                rain = 0
            else:
                rain = np.nan
        else:
            rain = 1
        for j in range(len(countyID)):
          if label==countyID[j]:
            #print(j+1,city,rain)
            # rainL.append({'county':city, 'rain':rain})
            rainL.append({'date': {'year': int(Y), 'month': int(M), 'day': int(D)}, 'station': station, 'feature': 'rain', 'id': id[j], 'hr_data': [{'hr_v':rain, 'hr_t': int(H), 'hr_flag': 0}], 
                      'county':city})
    return rainL

#All data except THC and NMHC
def crawl_EPA():
  site_list=['11','16','12','14','15','64','13','51','58','49','54','52','56','57','47','71','53','50','48','1','24','30','32','29','31','28','42','45','44','43','46','67','5','70','2','9','6','10','8','4','7','3','66','65','18','68','20','17','21','19','23','22','27','26','25','35','33','34','69','36','72','37','38','41','40','39','59','61','60','62','63','75','55','73','74','76']
  o_list = ['60','3','50','42','13','49','33','29','70','31','69','32','40','45','52','16','23','65','9','62','8','38','75','10','1','83','56','15','63','77','6','58','80','30','46','48','28','34','25','19','21','41','12','68','43','51','7','53','4','39','64','24','71','22','44','72','78','17','18','26','11','47','61','57','36','59','20','14','54','66','37','67','2','5','27','35']
  r = requests.get("https://data.epa.gov.tw/api/v1/aqx_p_432?limit=1000&api_key=9be7b239-557b-4c10-9775-78cadfc555e9&format=json", verify=False)
  a=json.loads(r.text)
  #print(a)
  Y=datetime.now().year
  M=datetime.now().month
  D=datetime.now().day
  H=datetime.now().hour
  site_feature = ['PM2.5','PM10','O3','CO','SO2','NO2','WindSpeed','WindDirec']#No NMHC in api
  EPA_list = []
  #client = MongoClient('140.116.164.187',27017)
  #client.test.authenticate("ryuk","kidlab95400")
  #db = client.AirP
  #coll = db.record3
  for i in range(len(a['records'])):#Search every station
    label=a['records'][i]
    for j in range(len(site_feature)):#Find value according to feature
      for k in range(len(o_list)):
        if (o_list[k] == label['SiteId']):
          FN=site_feature[j]
          hr_data_dict = {'hr_v':label[FN], 'hr_t': int(H), 'hr_flag': 0}
          #query = {'feature': site_feature[j], "id": str(site_list[i]), "date.year": int(year),"date.month": int(mon), "date.day": int(date)}
          #fp.write(site_feature[j]+'\n')
          #if coll.find_one(query) == None:
          mydict = {'date': {'year': int(Y), 'month': int(M), 'day': int(D)}, 'station': label['SiteName'], 'feature': FN, 'id': site_list[k], 'hr_data': [hr_data_dict],'county': label['County']}
          EPA_list.append(mydict)
  return EPA_list
                
#Grab NMHC information                     
def crawl_EPA_NMHC():
  r = requests.get("https://data.epa.gov.tw/api/v1/aqx_p_313?offset=0&limit=1000&api_key=d695558f-7f7d-453e-97fb-fc392a55c0a6", verify=False)
  a=json.loads(r.text)
  site_list=['12','14','64','13','58','49','52','57','47','71','31','28','42','45','46','67','70','2','6','10','8','3','66','65','18','68','17','22','27','34','41','40','39','61','60','75','73']
  o_list = ['50','42','49','33','70','31','32','45','52','16','1','83','56','15','6','58','30','46','28','34','25','41','12','68','43','51','53','24','71','17','57','36','59','14','54','67','5']
  #print(a)
  Y=datetime.now().year
  M=datetime.now().month
  D=datetime.now().day
  H=datetime.now().hour
  site_feature = ['Concentration']#NMHC
  NMHC_list = []
  #client = MongoClient('140.116.164.187',27017)
  #client.test.authenticate("ryuk","kidlab95400")
  #db = client.AirP
  #coll = db.record3
  for i in range(len(a['records'])):#Search every station
    label=a['records'][i]
    for j in range(len(site_feature)):#Find value according to feature
      for k in range(len(o_list)):
        if (o_list[k] == label['SiteId']):
          FN=site_feature[j]
          hr_data_dict = {'hr_v':label[FN], 'hr_t': int(H), 'hr_flag': 0}
          #query = {'feature': site_feature[j], "id": str(site_list[i]), "date.year": int(year),"date.month": int(mon), "date.day": int(date)}
          #fp.write(site_feature[j]+'\n')
          #if coll.find_one(query) == None:
          mydict = {'date': {'year': int(Y), 'month': int(M), 'day': int(D)}, 'station': label['SiteName'], 'feature': 'NMHC', 'id': site_list[k], 'hr_data': [hr_data_dict],
                    'county': label['County']}
          NMHC_list.append(mydict)
          #print(mydict)
  return  NMHC_list
  # print(len(NMHC_list))
                
#Grab THC information                 
def crawl_EPA_THC():
    r = requests.get("https://data.epa.gov.tw/api/v1/aqx_p_312?offset=0&limit=1000&api_key=d695558f-7f7d-453e-97fb-fc392a55c0a6", verify=False)
    #api_key may need to be updated regularly
    a=json.loads(r.text)
    #print(a)
    site_list=['12','14','64','13','58','49','52','57','47','71','31','28','42','45','46','67','70','2','6','10','8','3','66','65','18','68','17','22','27','34','41','40','39','61','60','75','73']
    o_list = ['50','42','49','33','70','31','32','45','52','16','1','83','56','15','6','58','30','46','28','34','25','41','12','68','43','51','53','24','71','17','57','36','59','14','54','67','5']
    Y=datetime.now().year
    M=datetime.now().month
    D=datetime.now().day
    H=datetime.now().hour
    site_feature = ['Concentration']#THC
    THC_list = []
    #client = MongoClient('140.116.164.187',27017)
    #client.test.authenticate("ryuk","kidlab95400")
    #db = client.AirP
    #coll = db.record3
    for i in range(len(a['records'])):#Search every station
        label=a['records'][i]
        for j in range(len(site_feature)):#Find value according to feature
          for k in range(len(o_list)):
            if (o_list[k] == label['SiteId']):
              FN=site_feature[j]
              hr_data_dict = {'hr_v':label[FN], 'hr_t': int(H), 'hr_flag': 0}
              #query = {'feature': site_feature[j], "id": str(site_list[i]), "date.year": int(year),"date.month": int(mon), "date.day": int(date)}
              #fp.write(site_feature[j]+'\n')
              #if coll.find_one(query) == None:
              mydict = {'date': {'year': int(Y), 'month': int(M), 'day': int(D)}, 'station': label['SiteName'], 'feature': 'NMHC', 'id': site_list[k], 'hr_data': [hr_data_dict],
                        'county': label['County']}
              THC_list.append(mydict)
    return  THC_list[0:37]
            #x = coll.insert_one(mydict)  
            #fp.write('Station_ID: '+str(site_list[i])+' ')
            #fp.write(str(x)+'\n')
            #else:
                #hr_data_list = coll.find_one(query)['hr_data']
                #hr_data_list.append(hr_data_dict)
                #newvalue = {"$set": {"hr_data" : hr_data_list}}
                #x = coll.update_one(query,newvalue)
                #fp.write('Station_ID: '+str(site_list[i])+' ')
                #fp.write(str(x)+'\n')
            
            #coll = db.record5
            #query = {'feature': site_feature[j], "id": i+1, "date.year": int(year),"date.month": int(mon), "date.day": int(date)}
            #if coll.find_one(query) == None:
                #mydict = {'date': {'year': int(Y), 'month': int(M), 'day': int(D)}, 'station': label['SiteName'], 'feature': FN, 'id': i+1, 'hr_data': [hr_data_dict]}
                #x = coll.insert_one(mydict)  
            #else:
                #hr_data_list = coll.find_one(query)['hr_data']
                #hr_data_list.append(hr_data_dict)
                #newvalue = {"$set": {"hr_data" : hr_data_list}}
                #x = coll.update_one(query,newvalue)
#Write to DB             
def intoDB():
  result = Mixall()
  client = MongoClient('140.116.164.187',27017)
  client.test.authenticate("ryuk","kidlab95400")
  db = client.AirP
  coll = db.record3
  time_stamp = (datetime.now()).strftime("%Y/%m/%d %H:%M:%S")
  fp = open("EPA_todb.txt", "a")
  fp.write(time_stamp+'\n')
  Y=datetime.now().year
  M=datetime.now().month
  D=datetime.now().day
  H=datetime.now().hour
  for i in range(len(result)):
    query = {'feature': result[i]['feature'], "id": result[i]['id'], "date.year": int(Y),"date.month": int(M), "date.day": int(D)}
    if coll.find_one(query) == None:
      fp.write(result[i]['feature']+'\n')
      x = coll.insert_one(result[i])
      fp.write('Station_ID: '+str(result[i]['id'])+' ')
      fp.write(str(x)+'\n')
    else:
      hr_data_list = coll.find_one(query)['hr_data']
      hr_data_list.append(result[i]['hr_data'])
      newvalue = {"$set": {"hr_data" : hr_data_list}}
      x = coll.update_one(query,newvalue)
      fp.write('Station_ID: '+str(result[i]['id'])+' ')
      fp.write(str(x)+'\n')
            
      coll = db.record5
      query = {'feature': result[i]['feature'], "id": result[i]['id'], "date.year": int(Y),"date.month": int(M), "date.day": int(D)}
      if coll.find_one(query) == None:
        x = coll.insert_one(result[i])  
      else:
        hr_data_list = coll.find_one(query)['hr_data']
        hr_data_list.append(result[i]['hr_data'])
        newvalue = {"$set": {"hr_data" : hr_data_list}}
        x = coll.update_one(query,newvalue)
        
#Consolidate data on all pollution sources from the Environmental Protection Agency        
def Mixall():
  THC_list = crawl_EPA_THC()
  NMHC_list = crawl_EPA_NMHC()
  all = crawl_EPA()
  print(len(all))
  all.extend(crawl_EPA_THC())
  all.extend(crawl_EPA_NMHC())
  all.extend(crawl_Rain())
  all.extend(crawl_weather())
  print(len(all))
  print(len(THC_list))
  print(len(NMHC_list))
  print(len(crawl_Rain()))
  print(len(crawl_weather()))
#  print(all[1])
  return all
                  
# EPA = crawl_EPA()
# THC = crawl_EPA_THC()
# NMHC = crawl_EPA_NMHC()
# rr = crawl_Rain()
# ww = crawl_weather()
#all = Mixall(crawl_EPA(),crawl_EPA_THC(),crawl_EPA_NMHC(),crawl_Rain(),crawl_weather())
# result = Mixall_weather(Mixall(crawl_EPA(),crawl_EPA_THC(),crawl_EPA_NMHC()),crawl_Rain(),crawl_weather())

#Put it into the DB and run the data every hour
sched = BlockingScheduler()
sched.add_job(intoDB, 'interval', minutes = 60)
sched.start()