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
kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

def kl():
    for active_stream in spark.streams.active:
        print("Stopping %s b, 5y killAll" % active_stream)
        active_stream.stop()

sc = SparkContext()



# схема данных
schema = StructType() \
    .add("icao24", StringType()) \
    .add("callsign", StringType()) \
    .add("org_country", StringType()) \
    .add("time_pos", IntegerType()) \
    .add("last_answ", IntegerType()) \
    .add("lon", FloatType()) \
    .add("lat", FloatType()) \
    .add("baro_alt", FloatType()) \
    .add("on_grd", BooleanType()) \
    .add("vel", IntegerType()) \
    .add("tr_track", FloatType()) \
    .add("vert_rate", FloatType()) \
    .add("geo_alt", FloatType()) \
    .add("squawk", StringType()) \
    .add("time", TimestampType())

# подгружаем aircraft_db.csv
air_df1 = spark.read.csv("./airdb/aircraft_db.csv",header=True,sep=",")
air_df1.printSchema()
air_df1.show(10, truncate=False)
air_df1 = air_df1.select(F.col("icao").alias("icao24"), 'regid', 'mdl', 'type', F.col('operator').alias('op1'))
air_df1.printSchema()
air_df1.show()
air_df1.count()

# Подгружаем aDB150121.csv
air_df2 = spark.read.csv("./airdb/aDB150121.csv",header=True,sep=",")
air_df2.printSchema()
air_df2.show(10, truncate=False)
air_df2 = air_df2.select("icao24", 'registration','manufacturername', 'model', 'built',  F.col('operatorcallsign').alias('op2'), "firstflightdate", "seatconfiguration","engines" )
air_df2.printSchema()
air_df2.show(10, truncate=False)
air_df2.count()

# No stream, read Kafka - потребуется для обучения  ML модели
raw_all_no_stream = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "fly5"). \
    option("startingOffsets", "earliest"). \
    load()

raw_all_no_stream.printSchema()
raw_all_no_stream.count()

all_flight_data = raw_all_no_stream \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset")
all_flight_data.printSchema()
all_flight_data.count()



# Напоминалка !!!!! str, default ‘inner’. One of inner, outer, left_outer, right_outer, leftsemi.
static_joined_allfldata = all_flight_data.join(air_df1,  'icao24', "left_outer")
static_joined_allfldata = static_joined_allfldata.join(air_df2,  'icao24', "left_outer")
static_joined_allfldata.printSchema()
static_joined_allfldata.count()
static_joined_allfldata.show(10, truncate=False)

static_joined_allfldata.persist()
static_joined_allfldata.createOrReplaceTempView("all_fl_data") # создадим представление

# Все самолеты в БД
aircraft_in_st_jo = static_joined_allfldata.groupBy("icao24", "callsign",  "mdl",  'model','type', 'built', 'op1', 'op2', 'built',"firstflightdate", "seatconfiguration","engines" ).\
    agg(F.max("time").alias("last_time")).orderBy("last_time", ascending=False)
aircraft_in_st_jo.show(30, truncate=False)
print('!'*50)
print('All aircraft in  DB  =  ' + str(aircraft_in_st_jo.count()))
print('!'*50)
t_max_vel_by_h = all_flight_data.groupBy("callsign",  "baro_alt").agg(F.max("vel").alias("max_vel")).orderBy("max_vel", ascending=False)
t_max_vel_by_h.show()

#t_max_vel_by_h100 = t_max_vel_by_h.selectExpr("callsign", "baro_alt", "max_vel",  "where  baro_alt<100")

#  Какая максимальная вертикальная скорость на высоте  < 100 м
max_neg_vert_rate_h250 = spark.sql("""select * from  all_fl_data  as asfd  where (asfd.baro_alt < 250  AND asfd.vert_rate < 0) order by  asfd.vert_rate ASC""")
max_neg_vert_rate_h250 = max_neg_vert_rate_h250.groupBy("icao24", "callsign",  "baro_alt",  "mdl",  'model','type').agg(F.min("vert_rate").alias("max_neg_vert_rate")).orderBy("max_neg_vert_rate", ascending=True)
print('Какие  максимальные  вертикальные скорости у ЛА на высоте  <250 м при снижении?')
max_neg_vert_rate_h250.show(40, truncate=False)
max_neg_vert_rate_h250.count()


#  Какая максимальная вертикальная скорость на высоте  любой
max_neg_vert_rate = spark.sql("""select * from  all_fl_data  as asfd  where  asfd.vert_rate < 0 order by  asfd.vert_rate ASC""")
max_neg_vert_rate = max_neg_vert_rate.groupBy("icao24", "callsign",  "baro_alt",  "mdl",  'model','type').agg(F.min("vert_rate").alias("max_neg_vert_rate")).orderBy("max_neg_vert_rate", ascending=True)
print('Какие  максимальные  вертикальные скорости у ЛА на любой высоте  при снижении?')
max_neg_vert_rate.show(40, truncate=False)
max_neg_vert_rate.count()

#  Какая максимальная  скорость ?
max_vel = static_joined_allfldata.groupBy("icao24", "callsign",  "baro_alt",  "mdl",  'model','type').agg(F.max("vel").alias("max_vel")).orderBy("max_vel", ascending=False)
max_vel.show()

#  Какая максимальная  скорость на посадке при h <250 ?
max_vel_h250 = spark.sql("""select * from  all_fl_data  as asfd  where (asfd.baro_alt < 250  AND asfd.vert_rate < 0) order by  asfd.vert_rate ASC""")
max_vel_h250 = max_vel_h250.groupBy("icao24", "callsign",  "baro_alt",  "mdl",  'model','type').agg(F.max("vel").alias("max_vel_on_landing")).orderBy("max_vel_on_landing", ascending=False)
print('Какие  максимальные   скорости у ЛА на высоте  <250 м при снижении?')
max_vel_h250.show(40, truncate=False)
max_vel_h250.count()



######################################################################### STREAM

# slowly"baro_alt", "vert_rate"
#функция, чтобы выводить на консоль вместо show()
def console_output_stream(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

raw_data = spark.readStream. \
format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "fly5"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "100"). \
    load()

#out = console_output_stream(raw_data, 5)
#out.stop()

temp = raw_data \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset")

temp.printSchema()
#out = console_output_stream(temp, 5)


##JOINS
def console_output_join(df, freq, out_mode):
    return df.writeStream.format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .option("checkpointLocation", "checkpoints/my_watermark_console_chk2") \
        .outputMode(out_mode) \
        .start()

stream_joined = temp.join(air_df1,  'icao24', "inner")
stream_joined = stream_joined.join(air_df2,  'icao24', "inner")
stream_joined.isStreaming
stream_joined.printSchema()


selected_static_joined = stream_joined.select('icao24',  'callsign', "baro_alt", "vert_rate",'vel', 'tr_track', 'regid','registration','mdl','type','time', 'manufacturername', 'model', 'built', 'op1', 'op2')
stream = console_output_join(selected_static_joined , 1, "update")
stream.stop()


