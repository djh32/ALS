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
mid_part = "/dw_ext/weibo_recmd/story/jianhang/ALS_time/%s/distribute/mid/"%(DATE)
uid_part = "/dw_ext/weibo_recmd/story/jianhang/ALS_time/%s/distribute/uid/"%(DATE)
#result dir
out_put_dir = "/dw_ext/weibo_recmd/story/jianhang/ALS_time/%s/predict_result/"%(DATE)



def main():
    spark = SparkSession.builder.appName("DJHtest") \
        .config("hive.exec.scratchdir", "/user_ext/weibo_recmd/hive-weibo_recmd") \
        .config("spark.sql.warehouse.dir", "/user_ext/weibo_recmd/spark-warehouse") \
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

def get_predict_result(spark, uid_index ,part_index, candi_num=100):
    out_real_dir = out_put_dir + str(uid_index)
    dir_part_mid = []
    for index in part_index:
        dir_part_mid.append(mid_part+"prediction="+str(index))        #mid_part uid_part 是全局变量。 输入路径
    df_mid = spark.read.parquet(*dir_part_mid)              #id ,features
    df_uid = spark.read.parquet(uid_part+"prediction="+str(uid_index))    #id ,features 

    # 手动一行行match
    mid_list = df_mid.collect()
    result_rd = df_uid.rdd.map(lambda a:get_one_user_score(mid_list, a, candi_num)).map(lambda tp: get_uid_candidate(tp)).saveAsTextFile(out_real_dir)

    #if df_uid.count() >1000000:
    #    # 拆分大数据 之后笛卡尔积计算
    #    all_rdd = spark.sparkContext.parallelize([])
    #    rd_uid_part_list = df_uid.rdd.randomSplit( [0.2, 0.2, 0.2, 0.2, 0.2] ) #拆成5份
    #    for par_uid_rdd in rd_uid_part_list:
    #        rd_uid_part_result = par_uid_rdd.cartesian(df_mid.rdd).map(lambda pair: (pair[0][0],[(pair[1][0],pair[0][1].dot(pair[1][1])) ] )).reduceByKey(lambda a,b:a+b).map(lambda a:get_one_candidate(a[1],a[0],candi_num))
    #        #((uid,uidfeatures),(mid,mid_features)) => (uid,(mid,score))
    #        all_rdd = all_rdd.union(rd_uid_part_result)
    #    all_rdd.map(lambda a:"%s"%(a)).saveAsTextFile(out_real_dir)


    ##拆分uid后笛卡尔积求结果   拆分防止heapout   
    #df_all= spark.createDataFrame(spark.sparkContext.parallelize([("","","","")]) ,["uid_features","mid_features","pair","score"]).toPandas()     #汇总
    #rd_list = df_uid.rdd.randomSplit( [0.2, 0.2, 0.2, 0.2, 0.2] ) #拆成5份
    #for par_uid_rdd in rd_list:
    #    temp_df = par_uid_rdd.cartesian(df_mid.rdd).toDF(['uid_features','mid_features']).toPandas()        #(uid1,f),(mid1,f)    (uid1,f),(mid2,f)
    #    temp_df['pair'] = temp_rdd.apply(lambda a:str(a[0][0]) + "_" +str(a[1][0]) ,axis=1)        #添加了pair列
    #    temp_df['score'] = temp_rdd.apply(lambda a:a[0][1].dot(a[1][1]) ,axis=1)                   #添加了score列
    #    df_all = df_all.append(temp_df, ignore_index=True)

    #df_all = df_all.drop(0)       #初始行 pandas
    #df_result = spark.createDataFrame(df_all,["uid_features","mid_features","pair","score"]).select("pair","score")  #( (uid_mid,score) , (uid_mid,score)   )
    
    # 手动一行行  按照parquet 输出 
    #result_rd = df_uid.rdd.flatMap(lambda a:get_one_user_score(mid_list, a, candi_num))        #map(lambda tp: "%s\t%s\t%.4f"%(tp[0],tp[1],tp[2]) ).saveAsTextFile(out_real_dir)
    #result_df = spark.createDataFrame(result_rd, ["uid" ,"mid", "score"] )
    #result_df.write.parquet(out_real_dir)

    
def predict_part_uid(spark, PREINDEX):
    #model_mid_dir = "/dw_ext/weibo_recmd/story/jianhang/ALS/kmeans_mod/mid" 
    #model_uid_dir = "/dw_ext/weibo_recmd/story/jianhang/ALS/kmeans_mod/uid"
    #model_uid = BisectingKMeansModel.load(model_uid_dir)
    #model_mid = BisectingKMeansModel.load(model_mid_dir)

    #center_mid = model_mid.clusterCenters() #np array list
    #center_uid = model_uid.clusterCenters() #np array

    ## 获得prediction不小于10 的分类的index  防止有1的情况
    ##large_num_cluster_index = get_large_mid_index(spark, 2)     #只要mid簇中数量大雨2的簇 防止1的坏点干扰
    ##
    ##large_num_cluster_index = [i for i in xrange(1,100)]      #全量
    #large_num_cluster_index = [i for i in xrange(0,100)]      #全量
    #center_of_this_part = center_uid[int(PREINDEX)]     #当前分类的中心点
    #part_index = get_center_top_score(center_of_this_part, center_mid, large_num_cluster_index, 100)    #得分最高的40个mid中心点的index

    ##  获得了top50的mid划分的
    ##接着对每一个划分求score 和得分，然后拼接取top100
    ##每个用户取top300 的候选
    part_index = [x for x in xrange(0,100)]
    get_predict_result(spark, PREINDEX, part_index, 300)
    #  

if __name__ == "__main__":
    PREINDEX = sys.argv[1]
    main()
