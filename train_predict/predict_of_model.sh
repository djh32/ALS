#!/bin/bash

SPARK_HOME=/data0/spark/spark-2.0.0-bin
dt=`date -d"1 day ago" +"%Y%m%d"`

if test $# -ge 1; then
  dt=$1
fi


submit() {
echo "param is $1"
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files /data0/spark/spark-2.2.0-bin/conf/hive-site.xml \
  --num-executors 20 \
  --driver-memory 20g \
  --executor-memory 16g \
  --executor-cores 4 \
  --conf spark.memory.fraction=0.7 \
  --conf spark.default.parallelism=320 \
  --conf spark.network.timeout=360 \
  --conf spark.rpc.lookupTimeout=360 \
  als_distribute_predict.py $1
}

main() { 
  #remove predict
  #hadoop fs -rmr "/dw_ext/weibo_recmd/story/jianhang/ALS/predict_result"
  echo "summit index is $1"
  #submit $1
}

main
