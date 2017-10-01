from __future__ import print_function
import sys
import ast
import redis
import datetime
import numpy as np

from pyspark import SparkContext
# from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext, Row, DataFrame
# from pyspark.sql import DataFrameReader
from pyspark.streaming.kafka import KafkaUtils

u = 'ubuntu'
p = 'ubuntu'
db = 'ubuntudb'
host = 'localhost'
port = "5432"


# # Lazily instantiated global instance of SparkSession
# def getSparkSessionInstance(sparkConf):
#     if ("sparkSessionSingletonInstance" not in globals()):
#         globals()["sparkSessionSingletonInstance"] = SparkSession \
#             .builder \
#             .config(conf=sparkConf) \
#             .getOrCreate()
#     return globals()["sparkSessionSingletonInstance"]
#
#
# def process(rdd):
#     try:
#         # Get the singleton instance of SparkSession
#         spark = getSparkSessionInstance(rdd.context.getConf())
#
#         df = spark.read.json(rdd)
#
#         # Creates a temporary view using the DataFrame
#         df.createOrReplaceTempView("temp")
#         # df.show()
#         sqlDF = spark.sql("SELECT id as wellid, field_name as field, dt, wellbore_vol as prod_vol FROM temp")
#         sqlDF.show()
#
#         sqlDF.write \
#             .format("org.apache.spark.sql.cassandra") \
#             .mode('append') \
#             .options(table="test", keyspace="downtime_estimation") \
#             .save()
#     except:
#         pass


# def write_to_postres_DB(rdd):
#     try:
#         # Get the singleton instance of SparkSession
#         spark = getSparkSessionInstance(rdd.context.getConf())
#
#         df = spark.read.json(rdd)
#
#         # Creates a temporary view using the DataFrame
#         df.createOrReplaceTempView("temp")
#         # df.show()
#
#         sqlDF = spark.sql("SELECT id, vp_hist, time_hist FROM temp")
#         sqlDF.show()
#
#         # sqlDF.write \
#         #     .format("org.postgresql_postgresql-42.1.1") \
#         #     .mode('append') \
#         #     .options(table="test", keyspace="downtime_estimation") \
#         #     .save()
#     except:
#         pass


def getEvents(tokens):
    # dt = datetime.datetime.strptime(tokens[0], "%Y-%m-%d %H:%M:%S")
    dt = tokens[0].decode()
    ID = tokens[1].decode()
    status = tokens[2].decode()
    vol_prod = tokens[3].decode()
    fieldID = tokens[4].decode()
    completion_type = tokens[5].decode()
    wellbore_vol = tokens[6].decode()
    field_name = tokens[7].decode()
    state = tokens[8].decode()
    rate = tokens[9].decode()
    return {'id': ID, 'time': dt, 'status': status, 'vol_prod': vol_prod, 'fieldID': fieldID,
            'completion_type': completion_type, 'wellbore_vol': wellbore_vol, 'field_name': field_name, 'state': state,
            'rate': rate}  # Note: the rate here is production per day and not


def getSeconds(t):
    t0 = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    return (t0 - datetime.datetime(1870, 1, 1)).total_seconds()


def seconds_between(d1, d2):
    d1 = datetime.datetime.strptime(d1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.datetime.strptime(d2, "%Y-%m-%d %H:%M:%S")
    return abs((d2 - d1).total_seconds())


def sortEvent(x, y):
    x1 = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    y1 = datetime.datetime.strptime(y, "%Y-%m-%d %H:%M:%S")
    if x1 < y1:
        earlier = x
    else:
        earlier = y
    return earlier
    # return x


def solveLinearSys(a, b):
    a = np.array(a)
    b = np.array(b)
    return np.linalg.solve(a, b)


def call_estimator_looper(rdd):
    for i in rdd:
        call_estimator(i)


def call_estimator(rdd):
    id = rdd['id']
    time = rdd['time']
    status = rdd['status']
    vol_p = rdd['vp']
    vol_w = rdd['vw']
    # rp = rdd['rop']
    field_id = rdd['fieldID']
    comp_type = rdd['comp']
    field = rdd['field_name']
    # state = rdd['state']

    ## point to hist_map in redis cache
    # hist_map = redis.StrictRedis(host=REDIS_DNS, port=6379, db=0, decode_responses=True)

    ## point to hist_map in distibuted redis cache
    hist_map = redis.StrictRedis(host=REDIS_DNS, port=6379, db=0, decode_responses=True)


    # Obtain well recent history from redis cache
    history = hist_map.hgetall(id)

    # read from redis for recent history data or set initial values based on field averages
    if history == {}:

        if hist_map.keys():
            # if redis is empty compute field averages for initialization parameters
            vp_hist_sum = [0, 0, 0, 0]
            on_dt_hist_sum = [0, 0, 0, 0]
            off_dt_hist_sum = [0, 0, 0, 0]
            count = 0
            for temp_id in hist_map.keys():
                history_avg = hist_map.hgetall(temp_id)
                hav1 = ast.literal_eval(history_avg['vp'])
                hav2 = ast.literal_eval(history_avg['on_list'])
                hav3 = ast.literal_eval(history_avg['off_list'])
                if field_id == ast.literal_eval(history_avg['fieldID']) and comp_type == ast.literal_eval(
                        history_avg['comp']):
                    if (len(hav1) + len(hav2) + len(hav3)) == 12:
                        count += 1
                        vp_hist_sum = [x + y for x, y in zip(vp_hist_sum, hav1)]
                        on_dt_hist_sum = [x + y for x, y in zip(on_dt_hist_sum, hav2)]
                        off_dt_hist_sum = [x + y for x, y in zip(off_dt_hist_sum, hav3)]

            if count > 0:
                vp_hist = [x / count for x in vp_hist_sum]
                on_dt_hist = [x / count for x in on_dt_hist_sum]
                off_dt_hist = [x / count for x in off_dt_hist_sum]
                time_hist = [
                    datetime.datetime.strptime(time.decode(), "%Y-%m-%d %H:%M:%S") - datetime.timedelta(seconds=60)]
            else:
                # Initializing empty list if hist_map on redis is empty for the current wellID
                time_hist = []
                vp_hist = []
                on_dt_hist = []
                off_dt_hist = []
        else:
            # Initializing empty list if hist_map on redis is empty for the current wellID
            time_hist = []
            vp_hist = []
            on_dt_hist = []
            off_dt_hist = []

    else:
        time_hist = ast.literal_eval(history['time'])
        vp_hist = ast.literal_eval(history['vp'])
        on_dt_hist = ast.literal_eval(history['on_list'])
        off_dt_hist = ast.literal_eval(history['off_list'])
        if status == history['status']:
            if status == 'on':
                history['status'] = 'off'
                if vp_hist:
                    vp_hist.pop(len(vp_hist) - 1)
                    off_dt_hist.pop(len(off_dt_hist) - 1)
            else:
                history['status'] = 'on'
                if vp_hist:
                    vp_hist.pop(len(vp_hist) - 1)
                    on_dt_hist.pop(len(on_dt_hist) - 1)



    # --------------------------
    # The estimator model
    # --------------------------
    if len(vp_hist) >= 3:  # Determine downtown estimate

        # determine delta_t
        delta_t = seconds_between(time, time_hist[-1])

        # append time and production volume
        time_hist.append(time)

        if status == 'off':  # update on_list and estimate downtime

            on_dt_hist.append(delta_t)
            vp_hist.append(vol_p)
            if len(on_dt_hist) > 4:
                on_dt_hist.pop(0)
                vp_hist.pop(0)

            # Determine start up time estimate
            r = [x / y for x, y in zip(vp_hist[-3:], on_dt_hist[-3:])]

            # Estimated inflow rate
            try:
                cc = solveLinearSys([[r[0], 1], [r[1], 1]], [r[1], r[2]])
                rn = cc[0] * r[-1] + cc[1]
                r_inflow = 0.5 * (rn + vol_p / delta_t) - vol_w / delta_t
                # r_inflow = rn - vol_w / delta_t
            except:
                r_inflow = vol_p / delta_t - vol_w / delta_t

            # # Estimated inflow rate
            # r_inflow = vol_p / delta_t - vol_w / delta_t

            # downtime estimate
            delta_t_n = r_inflow / vol_w

            # determine estimated time for next start up
            time_n = datetime.timedelta(seconds=delta_t_n) + datetime.datetime.strptime(time.decode(),
                                                                                        "%Y-%m-%d %H:%M:%S")

        else:  # update off_list and set identifier

            # update off list
            off_dt_hist.append(delta_t)
            if len(off_dt_hist) > 4:
                off_dt_hist.pop(0)

            # 'on' status identifier
            time_n = -777
            delta_t_n = -777

        # check list lengths and pop unneeded history to save memory space
        if len(time_hist) > 7:
            time_hist.pop(0)
            # vp_hist.pop(0)

        # update redis cache
        hist_map.hmset(id, {'id': id, 'status': status, 'time': time_hist, 'vp': vp_hist, 'on_list': on_dt_hist,
                            'off_list': off_dt_hist, 'time_n': time_n, 'delta_t_n': delta_t_n, 'fieldID': field_id,
                            'comp': comp_type, 'field': field})

    else:
        # make rough estimate, do updates and set identifiers
        if time_hist:
            if status == 'off':
                # determine delta_t
                delta_t = seconds_between(time, time_hist[-1])
                # update production volume list
                vp_hist.append(vol_p)
                # update on_list
                on_dt_hist.append(delta_t)
                # time_n = datetime.timedelta(seconds=delta_t) + datetime.datetime.strptime(time.decode(), "%Y-%m-%d %H:%M:%S")
                # delta_t_n = -2

                # Inflow rate
                r_inflow = vol_p / delta_t - vol_w / delta_t

                # downtime estimate
                delta_t_n = r_inflow / vol_w

                # Estimate up time
                time_n = datetime.timedelta(seconds=delta_t) + datetime.datetime.strptime(time.decode(),
                                                                                          "%Y-%m-%d %H:%M:%S")
            else:
                # determine delta_t
                delta_t = seconds_between(time, time_hist[-1])
                # update on_list
                off_dt_hist.append(delta_t)
                time_n = -1
                delta_t_n = -1
        else:
            time_n = -999
            delta_t_n = -999

        # update recent timestamp list
        time_hist.append(time)

        # update redis
        hist_map.hmset(id, {'id': id, 'status': status, 'time': time_hist, 'vp': vp_hist, 'on_list': on_dt_hist,
                            'off_list': off_dt_hist, 'time_n': time_n, 'delta_t_n': delta_t_n, 'fieldID': field_id,
                            'comp': comp_type, 'field': field})

    return hist_map.hgetall(id)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: streaming <bootstrap.servers>", file=sys.stderr)
        exit(-1)

    kafkaIP = str(sys.argv[1])

    sc = SparkContext(appName="event")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)

    # kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'event_data_topic': 1})
    # kafkaStream = KafkaUtils.createStream(ssc, kafkaIP + ':9092', 'spark-streaming', {'event_data_topic': 1})
    # kafkaStream = KafkaUtils.createDirectStream(ssc, ["event_data_topic"], {"bootstrap.servers": '54.156.225.243' + ':9092'})
    kafkaStream = KafkaUtils.createDirectStream(ssc, ["event_data_topic"], {"bootstrap.servers": kafkaIP + ':9092'})

    # counting number of messages in kafka stream
    kafkaStream.count().map(lambda x: 'Messages in this batch: %s' % x).pprint()

    # extracting data and counting
    tuple_rdd = kafkaStream.map(lambda x: x[1])
    # tuple_rdd.pprint()

    # converting 'string of tuple' to tuple and extracting string of dictionary
    json_string_rdd = tuple_rdd.map(lambda x: ast.literal_eval(x)[1])
    # json_rdd.pprint()

    # Convert 'string of dictionary' to dictionary
    json_rdd = json_string_rdd.map(lambda x: ast.literal_eval(x))
    # json_rdd.pprint()

    # json_rdd.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: call_estimator(x)))
    # result = json_rdd.map(lambda x: call_estimator(x))
    # json_rdd.foreachRDD(lambda x: x.foreachPartition(lambda y: call_estimator_looper(y)))
    # json_rdd.foreachRDD(lambda x: call_estimator(x))
    # result.pprint()
    json_rdd.map(lambda x: call_estimator(x))


    ssc.start()
    ssc.awaitTermination()
