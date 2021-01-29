#!/bin/bash
. ~/.bash_profile
SPARK_HOME=/data0/spark/spark-2.2.0-bin
dt=`date -d"1 day ago" +"%Y%m%d"`

submit() {
echo "get_info.py $1 $2 $3"
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files /data0/spark/spark-2.2.0-bin/conf/hive-site.xml \
  --num-executors 20 \
  --driver-memory 20g \
  --executor-memory 24g \
  --executor-cores 4 \
  --conf spark.memory.fraction=0.7 \
  --conf spark.default.parallelism=320 \
  --conf spark.network.timeout=360 \
  --conf spark.rpc.lookupTimeout=360 \
  get_info.py $1 $2 $3
}

main() {

  calcu_day=$1

  hadoop fs -rmr "/user_ext/jianhang3/ALS_short_video/$1/get_mid_info"

  day_stop=`date -d "$calcu_day 1 days ago" +%Y%m%d `
  day_begin=`date -d "$day_begin 40 days ago" +%Y%m%d `       #烨哥 vg去重能去重3个月内的。    候选可以出2个月， 保留时长不要超过一个月   原来是28 现在是40
  echo $day_begin
  echo $day_stop
  submit $day_begin $day_stop $calcu_day   # `>"log_inshell" 2>&1`

}

main $1
