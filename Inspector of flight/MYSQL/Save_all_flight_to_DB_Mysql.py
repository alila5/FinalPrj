import json
import datetime
from random import choice, randint, random
import requests
import time
import pymysql.cursors
import pygame

def alarm(file):
    pass
    pygame.init()
    song = pygame.mixer.Sound(file)
    clock = pygame.time.Clock()
    song.play()
    e = True
    while e:
        clock.tick(1)
        e = False
    pygame.quit()

def connect(con_data):
# Connect to MySQL with pymysql.cursors
    #print(con_data)
    try:
        connection = pymysql.connect(host=con_data['host'],#'172.30.8.10',
                             user=con_data['user'],
                             password=con_data['pwd'],
                             db=con_data['db'],
                             charset='utf8',#mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        #str='The connection is established'
    except:
        print(Exception)
        connection = None
        #str = "Can't connect to MySQL server"
    return connection

########################################################################   Producer #######################################################################

lon = 0 #55.7530  # Широта центральной точки  исследуемой зоны
lat = 0 #37.6215  #  Долгота ...
dlt = 180  # Размер прямоугольника на карте в градусах = dlt*2
delta_time = 3600*3
# Ниже к-т 1.5 - поправка на разницу длинны дуги по долготе и широте
#url = "https://opensky-network.org/api/states/all?lamin="+str(lon-dlt)+"&lomin="+str(lat-1.5*dlt)+"&lamax="+str(lon+dlt)+"&lomax="+str(lat+1.5*dlt)
url = "https://opensky-network.org/api/states/all?lamin="+str(lon-dlt)+"&lomin="+str(lat-dlt)+"&lamax="+str(lon+dlt)+"&lomax="+str(lat+dlt)

def ifNone (val):
    if val  is None or val == 'None':
        return None
    else:
        return val

def read_new_flight_data_from_API():
    resp = requests.get(url)
    result = resp.json()
    with open('res.txt', 'w') as outfile:
        json.dump(result, outfile)
    # print('*'*50)
    # print(result)
    # print('*' * 50)
    new_rec_dict = {}
    k = 0
    print('Now in flight', len(result['states']))
    for flight in result['states']:
        try:
            key = flight[0].strip()+'_'+flight[1].strip()+'_'+str(flight[3])
            # Перевели скорость  в м/c в км/ч, пвыставили временные метки как разницу от time
            new_rec_dict.update({key: { \
            "icao24": flight[0], "callsign": flight[1], "org_country": flight[2], "time_pos": result['time']-flight[3], \
            "last_answ": result['time']-flight[4], "lon": flight[5], "lat": flight[6], "baro_alt": flight[7], \
            "on_grd": flight[8], "vel": int(round(flight[9] * 3.6, 0)), "tr_track": flight[10],
            "vert_rate": flight[11], \
            "geo_alt": flight[13], "squawk": ifNone(flight[14]), "time": (result['time']+delta_time)\
        }})
        except Exception as e:
            print(e)
            k+=1
    print('BAD',k)
    return new_rec_dict

def SQL_INSERT (icao24,  callsign, org_country,  lastposupdate,  lastcontact, lat,   lon,  baroaltitude,
                onground,  velocity,  heading,  vertrate,  geoaltitude,  squawk,  utime, con):
    #print('SQL_INSERT')
    try:
        with con.cursor() as cursor:
            dt = datetime.datetime.utcfromtimestamp(utime).strftime('%Y-%m-%d %H:%M:%S')
            sql = 'INSERT INTO  hw01.st(icao24, callsign, org_country, lastposupdate, lastcontact, lat, lon, baroaltitude, onground, \
                  velocity, heading, vertrate, geoaltitude, squawk, time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s,%s)'
            cursor.execute(sql, (icao24, callsign, org_country, lastposupdate, lastcontact, lat, lon, baroaltitude, onground,  velocity,  heading,  vertrate,  geoaltitude,  squawk,  dt))
            con.commit()
    except Exception as e:
        print(Exception)
    finally:
        #print(f'SQL_SET_in_FOTO_full_info_ONE_record RUN TIME = {time.time()-t}')
        pass


def save_static_message (num, con_data):
    for i in range (1, num):
        t = time.time()
        try:  # т.к. иногода "выбивает"  API
            con = connect(con_data)
            print('!' * 20)
            print('        ',i)
            print('!' * 20)
            new_rec_dict = read_new_flight_data_from_API()
            for k in new_rec_dict.keys():
                r = new_rec_dict[k]
                # print('--' * 50)
                # print(str(r))
                # print('--' * 50)
                SQL_INSERT(r['icao24'],  r['callsign'], r['org_country'], r['time_pos'], r['last_answ'], r['lon'], r['lat'], r['baro_alt'],
                           r['on_grd'], r['vel'],  r['tr_track'], r['vert_rate'], r['geo_alt'], r['squawk'], r['time'], con)
            print('Save ok')
            con.close()
            dt = time.time() - t
            print('It took time', dt)
            alarm('good.mp3')
            time.sleep(10)  # Остановка на 10 с т.к. анонимное подключение
        except Exception as e:
            print(e)
            alarm('alarm.mp3')




con_data= {'host':'localhost', 'user':'root', 'pwd':'Slon1975', 'db':'hw01'}
save_static_message(20, con_data)



