#!/bin/bash
. ~/.bash_profile
SPARK_HOME=/data0/spark/spark-2.2.0-bin
dt=`date -d"1 day ago" +"%Y%m%d"`

if test $# -ge 1; then
  dt=$1
fi


submit() {
echo "als_model.py $1 $2 $3 $4 $5"
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files /data0/spark/spark-2.2.0-bin/conf/hive-site.xml \
  --num-executors 30 \
  --driver-memory 2g \
  --executor-memory 23g \
  --executor-cores 6 \
  --conf spark.memory.fraction=0.9 \
  --conf spark.memory.storageFraction=0.2 \
  --conf spark.default.parallelism=540 \
  --conf spark.network.timeout=360 \
  --conf spark.rpc.lookupTimeout=360 \
  --conf spark.yarn.executor.memoryOverhead=8g \
  als_train_model.py $1 $2 $3 $4 $5
  #--conf spark.task.maxFailures=1 \
  # spark_daily_played.py $dt
}

main() {
  #local logfile=logs/getdailyplayed_${dt}.log

  hadoop fs -rmr "/user_ext/jianhang3/ALS_short_video/$1/model/"
  hadoop fs -rmr "/user_ext/jianhang3/ALS_short_video/$1/checkpoint/"
  rank=256
  lmd=0.04
  iternum=10
  alhpa=1
  calcu_day=$1
  submit $rank $lmd $iternum $alhpa $1  # `>"log_inshell" 2>&1`

  ##训练测试
  #for rank in {"80","150","220"}
  #do
  #        for lmd in {"0.2","0.5","0.8"}
  #	  do
  #      	  for iternum in {"40","60","80"}
  #        	  do
  #     		  	submit $rank $lmd $iternum
  #      		sleep 2m     #防止最后输出没有als.py中的print
  #      		id=`grep "Submitting" log|awk '{print $7}'|tail -1`      #都写进log中，  最后一个才是新的
  #      		yarn logs -applicationId $id > logs_3/$id"_logs"
  #      	  done
  #        done
  #done
}

main $1
