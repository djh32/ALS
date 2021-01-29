#!/usr/bin/bash
. ~/.bash_profile

JAR_FILE="/data0/weibo_recmd/story/Algorithm/hadoop/story.jar"
CONF_FILE="/data0/weibo_recmd/story/Algorithm/hadoop/conf_export.xml"

function get_file(){
	dir=$1
	day=$2
	beginday=$3
	files=""
	for((i=$day; i>=1; i--))
	do
		DATE=`date -d "$beginday $i days ago" +%Y%m%d`
		file="$dir/dt=$DATE"
		hadoop fs -test -e $file
		if [ $? -ne 0 ]; then
		continue
		fi
		if [ "$files"x != ""x ]; then
		files=${files}","
		fi
	files="${files}${file}/*"
	done
	echo $files
}

function createTable(){
hive -e "
	create table if not exists jianhang_als_uploads_source(
	uid	string comment '候选的uid',
	midlist	string comment '候选mid') comment 'als过滤已读的结果'
	partitioned by (dt string comment '日期分区，20170424')
	row format delimited
	fields terminated by '\t'
	lines terminated by '\n'
	stored as textfile;
"

}

#createTable
day=$1
source=`get_file "/user_ext/weibo_recmd/warehouse/little_video_behaviour" 30 $day`  #候选去除30天的观看行为
echo $source
recommend="/user_ext/jianhang3/ALS_short_video/$1/predict_result_final/*/*"

result_dir="/user_ext/jianhang3/ALS_short_video/$1/remove_looked_mid_total"
hadoop fs -rmr $result_dir
hadoop jar $JAR_FILE datamining.als.AlsGetUidNotReadMidTotal -conf $CONF_FILE $source $recommend $result_dir 200
#hadoop fs -cat $result_dir/* >result

#hive -e "alter table jianhang_als_uploads_source add partition (dt=$day) location '$result_dir'"
#if [ $? -ne 0 ]; then
#	echo "jianhang_als_uploads_source $DATE error!"
#fi
