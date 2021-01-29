# coding:utf-8
from pyspark.sql import SparkSession
import datetime, sys, re
import zlib
import json
import traceback
import os

DATE = sys.argv[3]
# 去掉观看过的uid midlist 最大300 最小1 
REMOVE_LOOKED = "/user_ext/jianhang3/ALS_short_video/%s/remove_looked_mid_total"%(DATE)
out_dir = "/user_ext/jianhang3/ALS_short_video/%s/get_mid_info"%(DATE)
def main():
    spark = SparkSession.builder.appName("DJHtest") \
        .config("hive.exec.scratchdir", "/user_ext/jianhang3/hive-jianhang3") \
        .config("spark.sql.warehouse.dir", "/user_ext/jianhang3/spark-warehouse") \
        .config("hive.metastore.client.socket.timeout", 360) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    get_mid_info(spark, str(DAYBEGIN), str(DAYSTOP) )

def mid_to_uid(uid_midlist):
    rtn_list = []
    (uid, midlist) = uid_midlist.split("\t")
    index = 0
    for mid in midlist.split(","):
        rtn_list.append((mid, (index, uid)))       #[ (mid,(index, uid)),(mid,(index,uid))   ]
        index += 1
    return rtn_list
    
def change_uid(tup):   #(mid,((authid,type,object_id), (index,uid)  ))
    uid = tup[1][1][1]
    mid = tup[0]
    authid = tup[1][0][0]
    type = tup[1][0][1]
    obj_id = tup[1][0][2]
    index = tup[1][1][0]
    d = {}
    d['uid'] = authid          #authid
    d['type'] = type
    d['id'] = mid
    d['oid'] = obj_id
    d['index'] = index
    return [ (uid,json.dumps(d)) ] 

def cut_user_mid(uid_jsonlist):
    # a = "{uid:1,index:2,id:123,type:mid}"
    uid = uid_jsonlist[0]
    lis = json.loads(uid_jsonlist[1])
    #  最终上传lushan的 每个uid的候选数量上限
    sort = sorted(lis, key=lambda a:a['index'])[:200]
    rtn = []
    for d in sort:
        d.pop('index')
        #js_str = json.dumps(d)
        rtn.append(d)
    return [(uid,json.dumps(rtn))]
    


def get_mid_info(spark, DAYBEGIN, DAYSTOP):
    #mid_info_sql_str = "select material_id,material_uid,material_type,object_id from mds_short_video_material where dt between %s and %s and material_duration > 5"%(DAYBEGIN, DAYSTOP)
    #mid_info_sql_str = "select mid,uid,object_id from search_recommend_dm_material_recom_video_recommendable_shortvideo where dt between %s and %s and object_duration > 5"%(DAYBEGIN, DAYSTOP)
    mid_info_sql_str = "select mid,uid,object_id from search_recommend_dm_material_recom_video_recommendable_shortvideo where dt=%s and object_duration > 5"%(DAYSTOP)
    mid_info_rdd = spark.sql(mid_info_sql_str).rdd.flatMap(lambda a:[ (a.mid,(a.uid,"mid", a.object_id)) ] )       #[ (mid,(authid,type, object_id))  ]
    uid_midlist_rdd = spark.sparkContext.textFile(REMOVE_LOOKED).flatMap(lambda a:mid_to_uid(a) )     #[(mid,(index uid))]
    merge_rdd = mid_info_rdd.leftOuterJoin(uid_midlist_rdd).filter(lambda a: a[1][1]!=None).flatMap(lambda a:change_uid(a) ).reduceByKey(lambda a,b : a+","+b).flatMap(lambda a:[ (a[0],  "["+a[1]+"]") ]  )\
    .flatMap(lambda a:cut_user_mid(a))
    merge_rdd.map(lambda a:"%s\t%s"%(a[0],a[1])).saveAsTextFile(out_dir)
    # [ (mid,((authid,type), (index,uid))  )   ]   => [ (uid,json_str)] => [(uid,(js,js))] 
    


if __name__ == "__main__":
    DAYBEGIN= sys.argv[1]
    DAYSTOP = sys.argv[2]
    main()
