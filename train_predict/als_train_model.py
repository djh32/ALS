# coding:utf-8
from pyspark.sql import SparkSession
import datetime, sys, re
import traceback
import os
from pyspark.mllib.recommendation import ALS,  MatrixFactorizationModel, Rating


DATE = sys.argv[5]
#spark
model_dir="/user_ext/jianhang3/ALS_short_video/%s/model/"%(DATE)
factor_dic= "/user_ext/jianhang3/ALS_short_video/%s/factor/"%(DATE)

#真实训练
uid_mid_score_source ="/user_ext/jianhang3/ALS_short_video/%s/uid_mid_score/"%(DATE)
#训练效果测试用
#uid_mid_score_source ="/user_ext/jianhang3/ALS_short_video/%s/uid_mid_score/part-0007*"%(DATE)

uid_id_list = "/user_ext/jianhang3/ALS_short_video/%s/id_list/uid/"%(DATE)
mid_id_list = "/user_ext/jianhang3/ALS_short_video/%s/id_list/mid/"%(DATE)


def main():

    #.config("hive.exec.scratchdir", "/user_ext/weibo_recmd/hive-weibo_recmd") \
    #.config("spark.sql.warehouse.dir", "/user_ext/weibo_recmd/spark-warehouse") \
    spark = SparkSession.builder.appName("DJHtest") \
        .config("hive.exec.scratchdir", "/user_ext/jianhang3/hive-jianhang3") \
        .config("spark.sql.warehouse.dir", "/user_ext/jianhang3/spark-warehouse") \
        .config("hive.metastore.client.socket.timeout", 360) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    get_tag_info(spark, int(RANK), float(LMD), int(ITERNUM) ,float(ALPHA))

def change_mid(score_rdd, mid_rdd):     #(uid,mid.score)  (mid,id)
    temp_rdd = score_rdd.flatMap(lambda tp:[ (tp[1],(tp[0],tp[2])) ])  #(mid,(uid,score))
    temp_join = temp_rdd.leftOuterJoin( mid_rdd ).filter(lambda a: a[1][1]!=None).flatMap(lambda a: [ (a[1][0][0],a[1][1],a[1][0][1]) ])     #(uid,midchange,score) 
    return temp_join

def change_uid(score_rdd, uid_rdd):   #(uid,mid,score)      (uid,id)
    temp_rdd = score_rdd.flatMap(lambda tp:[ (tp[0],(tp[1],tp[2])) ])  #(uid,(mid,score))
    temp_join = temp_rdd.leftOuterJoin( uid_rdd ).filter(lambda a: a[1][1]!=None).flatMap(lambda a: [( a[1][1],a[1][0][0],a[1][0][1])] )     #(uidchange,midchange,score) 
    return temp_join

def get_tag_info(spark, RANK, LMD, ITERNUM, ALPHA):
    spark.sparkContext.setCheckpointDir("/user_ext/jianhang3/ALS_short_video/%s/checkpoint"%(DATE))
    #uid_mid_score = spark.sparkContext.textFile(uid_mid_score_source).flatMap(lambda a: [(int(a.split("\t")[0]), int(a.split("\t")[1]), float('%.2f'  %(float(a.split("\t")[2]))) )] )  
    uid_mid_score = spark.sparkContext.textFile(uid_mid_score_source).flatMap(lambda a: [(a.split("\t")[0], a.split("\t")[1], a.split("\t")[2]) ] )    #(uid,mid,score)
    uid_id = spark.sparkContext.textFile(uid_id_list).flatMap(lambda a:[(a.split("\t")[0],a.split("\t")[1].split("_")[0] ) ] )     #uid  id_uid   =>   (uid,id)
    mid_id = spark.sparkContext.textFile(mid_id_list).flatMap(lambda a:[(a.split("\t")[0],a.split("\t")[1].split("_")[0] ) ] )     #mid  id_mid   =>   (mid,id)
    changed_mid = change_mid(uid_mid_score, mid_id)
    
    #为了最后预测RMSE 的时候 可以看出效果 所以得分都乘以100    真实情况需要改回来
    #changed_all_id = change_uid(changed_mid, uid_id).flatMap(lambda a: [(int(a[0]), int(a[1]), float('%.2f'  %(float(a[2]))) )] )
    changed_all_id = change_uid(changed_mid, uid_id).flatMap(lambda a: [(int(a[0]), int(a[1]), float('%.2f'  %(float(a[2]) )) )] )
    
    #test
    #changed_all_id = spark.sparkContext.parallelize([(1,2,3),(1,1,2),(1,3,1),(2,1,4),(1,5,2),(2,2,4),(2,3,4)]).flatMap(lambda l:[ Rating(int(l[0]),int(l[1]),float(l[2]))   ])

    #[trans, test] = changed_all_id.randomSplit( [0.8,0.2] )
    #pre = test.flatMap(lambda a :[ ( a[0],a[1] )  ])     #(uid,mid,score) => (uid,mid)   pre
    #model = ALS_short_video/$1.train(trans, 50, lambda_=0.02, iterations=10 , nonnegative=True, seed=10)

    model = ALS.trainImplicit(changed_all_id, RANK, lambda_=LMD, iterations=ITERNUM, alpha=ALPHA, nonnegative=True)
    #如果不在训练集合的用户 没有结算结果
    #predictions = model.predictAll(pre).map(lambda r: ((r[0], r[1]), r[2]))       #( uid,mid  ),score       25g*0.2 =5g
    #test_tup = test.map(lambda r: ((r[0], r[1]), r[2]))                           #( uid,mid  ),score       25g*0.2 =5g
    
    #ratesAndPreds = predictions.leftOuterJoin(test_tup).filter(lambda a: a[1][1]!=None)
    #MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    #输出test真实数据  以及  test预测数据


    #api版本  过大了
    #ratesAndPreds = changed_all_id.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)

    #我的版本, 尽管左连但是仍过大
    #all_pair = changed_all_id.map(lambda r: ((r[0], r[1]), r[2]))
    #ratesAndPreds = predictions.leftOuterJoin( all_pair ).filter(lambda a: a[1][1]!=None)        #避免过大 超过17g container 就炸了。   只判断预测的对不对。越小越好
    
    #保留测试数据,之后分别拆成10份验证RMSE
    

    #MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    #print "DJH ALS_short_video/$1 TEST RANK:%d LMD:%.2f ITERNUM:%d MSE:%.2f "%(RANK, LMD, ITERNUM, MSE)
    print "DJH ALS TEST RANK:%d LMD:%.2f ITERNUM:%d "%(RANK, LMD, ITERNUM)

    #测试集合 以及 


    model.save(spark.sparkContext , model_dir)
    


if __name__ == "__main__":
    RANK = sys.argv[1]
    LMD = sys.argv[2]
    ITERNUM = sys.argv[3]
    ALPHA = sys.argv[4]
    main()
