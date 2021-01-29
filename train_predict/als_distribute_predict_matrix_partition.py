# coding:utf-8
from pyspark.sql import SparkSession
import datetime, sys, re
import traceback
import os
import numpy as np
from pyspark.ml.clustering import BisectingKMeansModel
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.linalg import Vectors

DATE = sys.argv[2]
#spark
mid_part = "/user_ext/jianhang3/ALS_short_video/%s/distribute/mid/"%(DATE)
uid_part = "/user_ext/jianhang3/ALS_short_video/%s/distribute/uid/"%(DATE)
#result dir
out_put_dir = "/user_ext/jianhang3/ALS_short_video/%s/predict_result/"%(DATE)
#out_put_dir = "/user_ext/jianhang3/ALS/%s/"%(DATE)



def main():
    spark = SparkSession.builder.appName("DJHtest") \
        .config("hive.exec.scratchdir", "/user_ext/jianhang3/hive-jianhang3") \
        .config("spark.sql.warehouse.dir", "/user_ext/jianhang3/spark-warehouse") \
        .config("hive.metastore.client.socket.timeout", 360) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    predict_part_uid(spark, PREINDEX)

#param : 一个用户类别的中心点向量， 一个全部mid中心点的list
def get_center_top_score(center_of_part, center_mid, large_num_cluster_index, top_mid_cluster):
    score = {}
    #for index in range(len(center_mid)):
    for index in large_num_cluster_index:
        score[index] = int(center_of_part.dot(center_mid[index]))
    temp_list = sorted(score.items(), key=lambda a:a[1] ,reverse=True)[:top_mid_cluster]

    rtn_list = []
    for (k,v) in temp_list:
        rtn_list.append(k)
    return rtn_list

def get_large_mid_index(spark, less_num ):
    mdf = spark.read.parquet(mid_part)
    mdf.createOrReplaceTempView("table1")
    df2 = spark.sql("SELECT prediction as f1, count(prediction) as f2 from table1 where group by prediction")      #predict, num(predict)
    rtn_index_list = df2.rdd.map(lambda a: (a[0]) if a[1]>less_num else () ).filter(lambda a:a!=()).collect()
    return rtn_index_list

def get_one_user_score(midlist, uid_tuple, candi_num):
    temp = {}
    uid=uid_tuple[0]
    uid_features = np.array(uid_tuple[1])     #1*256
    mid_features = np.array([x[1] for x in midlist])   #20w * 256
    result = uid_features.dot(mid_features.T)          # 1*20W 结果  index 就是mid在midlist中的index  
    midindex_score_list = []
    for x in xrange(0,len(result)):
        midindex_score_list.append((x,result[x]))

    #for (mid, feature) in midlist:
    #    score = uid_features.dot(np.array(feature).T)          #mid feature 转置
    #    temp[mid] = float(score)
    temp_list = sorted(midindex_score_list,key= lambda a:a[1], reverse=True)[:candi_num]
    rtn_list = []
    for (mid_index, score) in temp_list:
        rtn_list.append((midlist[mid_index][0], score))
    return (uid, rtn_list)      #(uid, [(mid,score),(mid,score)])

def get_one_candidate(mid_list, uid, candi_num):
    score_dic = {}
    for (mid, score) in mid_list:
        score_dic[mid] = score
    score_list = sorted(score_dic.items(),key= lambda a:a[1], reverse=True)[:candi_num]
    
    rtn_list = []
    for (mid, score) in score_list:
        rtn_list.append(str(mid) + ":" + "%.2f"%(score))
    rtn_str = "\t".join(rtn_list)
    return str(uid)+"_"+rtn_str

def get_uid_candidate(tup):       #(uid, [(mid,score), (mid,score)])
    mid_list = []
    uid = tup[0]
    for (mid, score) in tup[1]:
        temp = str(mid) + ":" + "%.2f"%(score)
        mid_list.append(temp)
    return str(uid) + "_" + "\t".join(mid_list)        #uid_mid1:score    mid2:score

def get_top_mid_for_user(candi_num, result, midlist):
    midindex_score_list = []
    for x in xrange(0,len(result)):      #0-10W
        midindex_score_list.append((x,result[x]))
    temp_list = sorted(midindex_score_list,key= lambda a:a[1], reverse=True)[:candi_num]
    rtn_list = []
    for (mid_index, score) in temp_list:
        rtn_list.append((midlist[mid_index][0], score))    #[ (mid,score), (mid,score)]
    return rtn_list

def partition_get_score(iterator, midlist, candi_num):
    rtn_list = []
    uid_list = []
    uid_vector_matrix = []
    for (uid, uids_vector) in iterator:
        uid_list.append(uid)
        uid_vector_matrix.append(uids_vector)

    matrix_user = np.array(uid_vector_matrix)     #200 * 256
    matrix_item = np.array([x[1] for x in midlist])   #10w * 256
    result = list(matrix_user.dot(matrix_item.T))    #200 * 10W
    for i in xrange(0,len(result)):
        mid_score_list = get_top_mid_for_user(candi_num, result[i], midlist)
        rtn_list.append( (uid_list[i], mid_score_list))
    return iter(rtn_list)

def get_predict_result(spark, uid_index ,part_index, candi_num):
    out_real_dir = out_put_dir + str(uid_index)
    dir_part_mid = []
    for index in part_index:
        dir_part_mid.append(mid_part+"prediction="+str(index))        #mid_part uid_part 是全局变量。 输入路径
    df_mid = spark.read.parquet(*dir_part_mid)              #id ,features      以list 为变量 输入list中全部数据
    df_uid = spark.read.parquet(uid_part+"prediction="+str(uid_index))    #id ,features 

    ## 手动一行行match
    #mid_list = df_mid.collect()
    #result_rd = df_uid.rdd.map(lambda a:get_one_user_score(mid_list, a, candi_num)).map(lambda tp: get_uid_candidate(tp)).saveAsTextFile(out_real_dir)

    # repartition 以后划分 8W 一块的用户 与 mid 10W量级的 点积
    mid_list = df_mid.collect()
    result_rd = df_uid.rdd.coalesce(400,False).mapPartitions(lambda a:partition_get_score(a,mid_list, candi_num)).map(lambda tp: get_uid_candidate(tp)).saveAsTextFile(out_real_dir)       #8W个 uid 分成400个part  同一个part里面有200个uid,vector
    #result_rd = df_uid.rdd.map(lambda a:get_one_user_score(mid_list, a, candi_num)).map(lambda tp: get_uid_candidate(tp)).saveAsTextFile(out_real_dir)
def predict_part_uid(spark, PREINDEX):
    ##每个用户取top300 的候选
    part_index = [x for x in xrange(0,100)]
    get_predict_result(spark, PREINDEX, part_index, 300)
    #  

if __name__ == "__main__":
    PREINDEX = sys.argv[1]
    main()
