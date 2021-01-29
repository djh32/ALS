#!/usr/bin/bash
. ~/.bash_profile

JAR_FILE="/data0/weibo_recmd/story/Algorithm/hadoop/story.jar"
CONF_FILE="/data0/weibo_recmd/story/Algorithm/hadoop/conf_export.xml"


# 日期
calcu_day=$1
before_day=`date -d "$calcu_day 1 days ago" +%Y%m%d`
# 目录
score="/user_ext/jianhang3/ALS_short_video/$calcu_day/get_mid_info/*"

# 这个才是保留的结果
result_dir="/user_ext/jianhang3/ALS_short_video/$calcu_day/data_base"
before_base="/user_ext/jianhang3/ALS_short_video/$before_day/data_base"
#执行   用昨天的database 以及今天的变化增量， 更新数据 并保留到今天的记录中
hadoop fs -rmr $result_dir
hadoop jar $JAR_FILE datamining.als.AlsDataBaseMerge -conf $CONF_FILE $score $before_base/* $result_dir 200
#hadoop fs -cat $result_dir/* >result

