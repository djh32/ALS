# coding:utf-8
from pyspark.sql import SparkSession
import datetime, sys, re
import traceback
import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

#spark
model_dir="/dw_ext/weibo_recmd/story/jianhang/ALS/model/"
uid_id_list = "/dw_ext/weibo_recmd/story/jianhang/ALS/id_list/uid/"
mid_id_list = "/dw_ext/weibo_recmd/story/jianhang/ALS/id_list/mid/"

#result dir
out_dir = "/dw_ext/weibo_recmd/story/jianhang/ALS/predict_result"
def main():
    spark = SparkSession.builder.appName("DJHtest") \
        .config("hive.exec.scratchdir", "/user_ext/weibo_recmd/hive-weibo_recmd") \
        .config("spark.sql.warehouse.dir", "/user_ext/weibo_recmd/spark-warehouse") \
        .config("hive.metastore.client.socket.timeout", 360) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    get_tag_info(spark, int(RANK), float(LMD), int(ITERNUM) )

def change_back_mid(result_rdd, mid_rdd):      #(id_uid ,id_mid, score   )   (id_mid, mid)    => ( id_uid, mid ,score  )
    temp_rdd = result_rdd.flatMap(lambda tp:[ (tp[1],(tp[0],tp[2])) ])  #(id_mid,(id_uid,score))
    temp_join = temp_rdd.leftOuterJoin( mid_rdd ).filter(lambda a: a[1][1]!=None).flatMap(lambda a: [ (a[1][0][0],a[1][1],a[1][0][1]) ])     #(id_uid, mid, score)
    return temp_join

def change_back_uid(result_rdd, uid_rdd):   #(id_uid, mid, score)  (id_uid, uid) 
    temp_rdd = result_rdd.flatMap(lambda tp:[ (tp[0],(tp[1],tp[2])) ])  #(id_uid,(mid,score))
    temp_join = temp_rdd.leftOuterJoin( uid_rdd ).filter(lambda a: a[1][1]!=None).flatMap(lambda a: [ (a[1][0][0],a[1][1],a[1][0][1]) ])     #(uid, mid, score)
    return temp_join

def get_tag_info(spark, RANK, LMD, ITERNUM):
    model = MatrixFactorizationModel.load(spark, "/dw_ext/weibo_recmd/story/jianhang/ALS/model/")
    #change back result rdd
    result = model.recommendProductsForUsers(100).map(lambda a:(str(a[0]) , str(a[1]) ,str(a[2])   ))     #(id_uid1 ,(id_uid1, id_mid, score),(id_uid1,id_mid,score) ) ,(id_uid2,(id_uid2,id_mid,score),(id_uid2,id_mid,score)   ) 
    uid_id = spark.sparkContext.textFile(uid_id_list).flatMap(lambda a:[(a.split("\t")[1].split("_")[0] , a.split("\t")[0]) ] )     #uid id_uid   =>   (id, uid)
    mid_id = spark.sparkContext.textFile(mid_id_list).flatMap(lambda a:[(a.split("\t")[1].split("_")[0] , a.split("\t")[0]) ] )     #mid id_mid   =>   (id,mid)
    #修改下面！！
    tup_list = result.flatMap(lambda a: a[1] )     #flat 去除了result 中对用户的元组     => (id_uid ,id_mid, score   )
    changed_mid = change_back_mid(tup_list, mid_id)
    changed_uid = change_back_uid(changed_mid, uid_id)
    #out_put
    print "DJH ALS_PREDICT RANK:%d LMD:%.2f ITERNUM:%d "%(RANK, LMD, ITERNUM)
    changed_uid.map(lambda tp:"%s\t%s\t%s" %(tp[0],tp[1],tp[2])).saveAsTextFile(out_dir)


if __name__ == "__main__":
    RANK = sys.argv[1]
    LMD = sys.argv[2]
    ITERNUM = sys.argv[3]
    main()
