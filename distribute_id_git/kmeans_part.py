# coding:utf-8
from pyspark.sql import SparkSession
import datetime, sys, re
import traceback
import os
import random
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.linalg import Vectors

DATE = sys.argv[3]
#spark
model_dir="/user_ext/jianhang3/ALS_short_video/%s/model/"%(DATE)

#result dir
out_dir = "/user_ext/jianhang3/ALS_short_video/%s/distribute/"%(DATE)
def main():
    #.config("hive.exec.scratchdir", "/user_ext/weibo_recmd/hive-weibo_recmd") \
    #.config("spark.sql.warehouse.dir", "/user_ext/weibo_recmd/spark-warehouse") \
    spark = SparkSession.builder.appName("DJHkmeans") \
        .config("hive.exec.scratchdir", "/user_ext/jianhang3/hive-jianhang3") \
        .config("spark.sql.warehouse.dir", "/user_ext/jianhang3/spark-warehouse") \
        .config("hive.metastore.client.socket.timeout", 360) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    get_tag_info(spark, int(K_CLUSTER), int(ITERNUM))

def get_model(spark, src_dir, model_info, out_str):
    source = spark.read.parquet(src_dir)    #row(id bigint, features array<double>)  DF
    rdd_source = source.rdd            #to rdd  (id, feature)
    df_real = rdd_source.map(lambda a:( a[0],a[1],random.randint(0,99) )).toDF(['id','features','prediction'])         #row(id bigint, features DenseVector)
    df_real.write.partitionBy("prediction").parquet(out)
    #model = model_info.fit(df_real)

    ##这个是划分了
    #df_mid = model.transform(df_real)       # df  (id, feature, prediction)
    #out = out_dir + out_str
    #df_mid.write.partitionBy("prediction").parquet(out)

    #rtn_str = "This is "+ out_str +" computeCost: " + str(model.computeCost(df_real))
    #return model,rtn_str

def get_model_rand_shaff(spark, src_dir,  out_str):
    source = spark.read.parquet(src_dir)    #row(id bigint, features array<double>)  DF
    rdd_source = source.rdd            #to rdd  (id, feature)
    df_real = rdd_source.map(lambda a:( a[0],a[1],random.randint(0,99) )).toDF(['id','features','prediction'])         #row(id bigint, features DenseVector)
    out = out_dir + out_str
    df_real.write.partitionBy("prediction").parquet(out)

def get_tag_info(spark, K_CLUSTER, ITERNUM):
    uid_path = model_dir + "data/user"
    mid_path = model_dir + "data/product"
    get_model_rand_shaff(spark, mid_path, "mid")
    get_model_rand_shaff(spark, uid_path, "uid")

    
    #bkm_mid = BisectingKMeans(k=K_CLUSTER, maxIter=ITERNUM)
    #bkm_mid = BisectingKMeans(k=100, maxIter=3)
    #bkm_uid = BisectingKMeans(k=100, maxIter=3)

    #model_mid,temp_str = get_model(spark, mid_path, bkm_mid, "mid")
    #print temp_str + " K is %d, ITERNUM is %d"%(K_CLUSTER, ITERNUM)
    #model_uid,temp_str = get_model(spark, uid_path, bkm_uid, "uid")

    #model_mid.save("/user_ext/jianhang3/ALS/kmeans_mod/mid")
    #model_uid.save("/user_ext/jianhang3/ALS/kmeans_mod/uid")

if __name__ == "__main__":
    K_CLUSTER = sys.argv[1]
    ITERNUM = sys.argv[2]
    main()
