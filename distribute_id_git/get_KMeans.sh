#!/bin/bash
. ~/.bash_profile

SPARK_HOME=/data0/spark/spark-2.2.0-bin
dt=`date -d"1 day ago" +"%Y%m%d"`

if test $# -ge 1; then
  dt=$1
fi


submit() {
echo "kmeans_part.py $1 $2 $3"
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
  kmeans_part.py $1 $2 $3
}

main() { 
  #remove 
  hadoop fs -rmr "/user_ext/jianhang3/ALS_short_video/$1/kmeans_mod/"
  hadoop fs -rmr "/user_ext/jianhang3/ALS_short_video/$1/distribute/"
  #mid 的迭代
  day=$1
  submit 100 5 $day

  #iternum=3
  ##训练测试
  #for k_cluster_num in {1024,2048,4096}
  #do
  #			submit $k_cluster_num $iternum
  #			sleep 2m     #防止最后输出没有als.py中的print
  #			id=`grep "Submitting" log|awk '{print $7}'|tail -1`      #都写进log中，  最后一个才是新的
  #			yarn logs -applicationId $id > logs/$id"_logs"
  #done
}

main $1
