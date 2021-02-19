#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[1]
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType, BooleanType, LongType
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
import json
import datetime
from random import choice, randint, random
import requests
import time

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()
kafka_brokers = "bigdataanalytics-worker-1.novalocal:6667"

def kl():  #  KILL ALL
    for active_stream in spark.streams.active:
        print("Stopping %s b, 5y killAll" % active_stream)
        active_stream.stop()

sc = SparkContext()

########################################################################   Producer #######################################################################

lon = 55.7530  # Широта центральной точки  исследуемой зоны
lat = 37.6215  #  Долгота ...
dlt = 2  # Размер прямоугольника на карте в градусах = dlt*2

# Ниже к-т 1.5 - поправка на разницу длинны дуги по долготе и широте
url = "https://opensky-network.org/api/states/all?lamin="+str(lon-dlt)+"&lomin="+str(lat-1.5*dlt)+"&lamax="+str(lon+dlt)+"&lomax="+str(lat+1.5*dlt)

def read_new_flight_data_from_API():
    resp = requests.get(url)
    result = resp.json()
    print('*'*50)
    print(result)
    print('*' * 50)
    new_rec_dict = {}
    for flight in result['states']:
        key = flight[0].strip()+'_'+flight[1].strip()+'_'+str(flight[3])
        # Перевели скорость  в м/c в км/ч, пвыставили временные метки как разницу от time
        new_rec_dict.update({key: { \
        "icao24": flight[0], "callsign": flight[1], "org_country": flight[2], "time_pos": result['time']-flight[3], \
        "last_answ": result['time']-flight[4], "lon": flight[5], "lat": flight[6], "baro_alt": flight[7], \
        "on_grd": flight[8], "vel": int(round(flight[9] * 3.6, 0)), "tr_track": flight[10],
        "vert_rate": flight[11], \
        "geo_alt": flight[13], "squawk": flight[14], "time": result['time'] \
        }})
    return new_rec_dict


def save_static_message (num):
    try:  # т.к. иногода "выбивает"  API
        for i in range (1, num):
            print('!' * 20)
            print(i)
            print('!' * 20)
            new_rec_dict = read_new_flight_data_from_API()
            for key in new_rec_dict.keys():
                print('--' * 50)
                print(str(new_rec_dict[key]))
                print('--' * 50)
                rdd = sc.parallelize([json.dumps(new_rec_dict[key])])
                bu_zdf = spark.read.json(rdd)
                bu_zdf.show(truncate=False)
                df = bu_zdf.selectExpr("CAST(null AS STRING) as key", "CAST(to_json(struct(*)) AS STRING) as value")
                df.show(truncate=False)
                df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_brokers) \
                .option("topic", "fly6") \
                .save()
    except Exception as e:
        print(e)
    time.sleep(10)  # Остановка на 10 с т.к. анонимное подключение

save_static_message(2)



