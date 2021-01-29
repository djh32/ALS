# coding:utf-8
from pyspark.sql import SparkSession
import datetime, sys, re
import traceback
import os
DATE = sys.argv[2]
#in
uid_id_list = "/user_ext/jianhang3/ALS_short_video/%s/id_list/uid/"%(DATE)
mid_id_list = "/user_ext/jianhang3/ALS_short_video/%s/id_list/mid/"%(DATE)
input_scoure = "/user_ext/jianhang3/ALS_short_video/%s/predict_result/"%(DATE)
#result dir
out_dir = "/user_ext/jianhang3/ALS_short_video/%s/predict_result_final/"%(DATE)


def main():
    spark = SparkSession.builder.appName("DJHtest") \
        .config("hive.exec.scratchdir", "/user_ext/jianhang3/hive-jianhang3") \
        .config("spark.sql.warehouse.dir", "/user_ext/jianhang3/spark-warehouse") \
        .config("hive.metastore.client.socket.timeout", 360) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    change_back_id(spark, PREINDEX)

def change(tup, order_lis):
    mid_dic = dict(order_lis)
    rtn_list = []
    uid = tup[0]
    mid_list = tup[1].split("\t")
    for temp_str in mid_list:  #str=>mid:score
        mid = temp_str.split(":")[0]
        #score = temp_str.split(":")[1]
        real_mid = str( mid_dic[mid]  )
        rtn_list.append(real_mid)
    return (uid, ",".join(rtn_list))      #(uid,"mid,mid,mid,")

def change_back_id(spark, PREINDEX):
    putin = spark.sparkContext.textFile(input_scoure + PREINDEX).map(lambda a:( a.split("_")[0] ,a.split("_")[1] )   )             #(uid,mid:score\tmid:score)
    out_real = out_dir+PREINDEX
    uid_id = spark.sparkContext.textFile(uid_id_list).flatMap(lambda a:[(a.split("\t")[1].split("_")[0] , a.split("\t")[0]) ] )     #uid id_uid   =>   (id, uid)
    mid_id = spark.sparkContext.textFile(mid_id_list).flatMap(lambda a:[(a.split("\t")[1].split("_")[0] , a.split("\t")[0]) ] )     #mid id_mid   =>   (id,mid)
    mid_list = mid_id.collect()
    result = putin.map(lambda a:change(a, mid_list)).leftOuterJoin(uid_id).filter(lambda a: a[1][1]!=None).map(lambda a:str(a[1][1]) + "_" + a[1][0]).saveAsTextFile(out_real)

if __name__ == "__main__":
    PREINDEX = sys.argv[1]
    main()
