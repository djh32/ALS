#!/bin/bash
. ~/.bash_profile

SPARK_HOME=/data0/spark/spark-2.2.0-bin

#线程池
process_pool(){
    #判断输入参数等
    if [ $# -lt 3 ]; then
        echo "$0 process_num command [args]"
        return 1
    fi
    _process_num=$1
    shift
    _func=$1
    shift
    if [[ ! $_process_num =~ ^[0-9]+$ ]]; then
        echo "process_num must be a number"
        return 1
    fi
    if !type $_func >/dev/null 2>&1; then
        echo "comannd must be executable"
        return 1
    fi
 
    # 创建一个先进先出的管道文件
    fifo="/tmp/$$.fifo"
    mkfifo $fifo
    #创建一个文件描述符号，把FD这个文件描述符关联到这个文件
    #{FD}表示非显示的描述符
    exec {FD}<>$fifo
    rm $fifo
  
 
    # 创建槽位
    for i in $(seq $_process_num); do
        echo >&$FD
    done
  
 
    # 执行具体命令
    for arg in $@; do
        read -u $FD
        {
            $_func $arg 
            echo >&$FD
        }&
    done
 
    # wait等待所有后台进程执行完成
    wait
 
    # 释放文件描述符
    exec {FD}>&-
}

test(){
	echo $1
	sleep 3
	return 0
}

submit() {
echo "param is $1 $day"
hadoop fs -rmr "/user_ext/jianhang3/ALS_short_video/$day/predict_result/$1"
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files /data0/spark/spark-2.2.0-bin/conf/hive-site.xml \
  --num-executors 20 \
  --driver-memory 8g \
  --executor-memory 15g \
  --executor-cores 6 \
  --conf spark.memory.fraction=0.9 \
  --conf spark.memory.storageFraction=0.1 \
  --conf spark.default.parallelism=360 \
  --conf spark.network.timeout=360 \
  --conf spark.rpc.lookupTimeout=360 \
  --conf spark.yarn.executor.memoryOverhead=128m \
  als_distribute_predict_matrix_partition.py $1 $day

hadoop fs -rmr "/user_ext/jianhang3/ALS_short_video/$day/predict_result_final/$1"
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files /data0/spark/spark-2.2.0-bin/conf/hive-site.xml \
  --num-executors 10 \
  --driver-memory 8g \
  --executor-memory 6g \
  --executor-cores 4 \
  --conf spark.memory.fraction=0.9 \
  --conf spark.default.parallelism=160 \
  --conf spark.network.timeout=360 \
  --conf spark.rpc.lookupTimeout=360 \
  get_real_mid_uid.py $1 $day
}

#如果昨日没成功的时候就开始今日的了，kill昨日
#sh kill_job.sh       #kill 之前的als_distribute_predict.py
#sleep 10s
#当日新进程
day=$1
process_pool 4 submit {0..99} #0-99  #1 2 3 4 5 6 7
#process_pool 1 submit 98 #0-99  #1 2 3 4 5 6 7

