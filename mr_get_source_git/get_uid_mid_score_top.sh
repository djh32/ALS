#!/usr/bin/bash
. ~/.bash_profile

JAR_FILE="/data0/weibo_recmd/story/Algorithm/hadoop/story.jar"
CONF_FILE="/data0/weibo_recmd/story/Algorithm/hadoop/conf_export.xml"

DIR="/data0/weibo_recmd/story/Algorithm/project"
function alert(){
	object=$1           # 主题
	message=$2          # 短信内容
	mail=$2             # 邮件内容
	receivers="jianhang3"       # 收件人，使用邮箱前缀即可，多个收件人使用逗号分割
	python $DIR/alert.py --service 'shortvideo' --object "$object" --subject "$message" --content "$mail" --msgto "$receivers" --mailto "$receivers"
}

source_file="/user_ext/jianhang3/ALS_short_video/$1/data_base/*"
function get_file(){
	dir=$1
	day=$2
	files=""
	for((i=$day; i>=1; i--))
	do
		DATE=`date -d "$i days ago" +%Y%m%d`
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


#根据用户在7天内对30天优质mid的行为获取得分   uid_mis_score  
#source=`get_file "/user_ext/weibo_recmd/warehouse/little_video_behaviour" 8`  #前8-前2天 总共7天     ver1
source=`get_file "/user_ext/weibo_recmd/warehouse/little_video_behaviour" 8`  #前7-前2天 总共6天	ver2
echo "input_source_is:"$source
#  得到7天内观看高于1000 的mid || 转评赞>100  否则计算没意义
result="/user_ext/jianhang3/ALS_short_video/$1/good_mid"
hadoop fs -rmr $result
hadoop jar $JAR_FILE datamining.als.AlsGetGoodMid -conf $CONF_FILE $source $result 100
hadoop fs -cat $result/* > goodmid_original

hadoop fs -test -e $result
if [ $? -ne 0 ]; then
	echo "$result not exist!"
	alert "cand_als_vert" "little_video_behaviour not exists!"
	exit
fi

result_num=`hadoop fs -dus $result | awk '{print $1}'`
if [ $result_num -eq 0 ]; then
	echo "$result fail!"
        alert "cand_als_vert" "little_video_behaviour not exists!"
	exit
fi

python get_good_mid.py goodmid_original       # 削弱热门mid
cat ./sort_result|awk '{print $1}' >goodmid


#  计算uid mid score     用户对 白名单中mid 的得分， 根据观看行为，点赞吗，转发，评论获得得分
result_dir="/user_ext/jianhang3/ALS_short_video/$1/uid_mid_score/"
hadoop fs -rmr $result_dir
hadoop jar $JAR_FILE datamining.als.AlsUidMidScore -D midfile=goodmid -files goodmid -conf $CONF_FILE $source $result_dir 200

#  为每一个mid 或者 uid  计算唯一id  为了能训练
#result_dir="/user_ext/jianhang3/ALS_short_video/$1/uid_mid_score/"
result_dir_of_id="/user_ext/jianhang3/ALS_short_video/$1/id_list/"
hadoop fs -rmr $result_dir_of_id
hadoop jar $JAR_FILE datamining.als.AlsGetCalcuId -conf $CONF_FILE $result_dir $result_dir_of_id 100
#hadoop fs -cat $result_dir_of_id/* >temp_result_id

#dump
mv uid_list uid_list_last
mv mid_list mid_list_last
hadoop fs -cat "/user_ext/jianhang3/ALS_short_video/$1/id_list/uid/*" >uid_list
hadoop fs -cat "/user_ext/jianhang3/ALS_short_video/$1/id_list/mid/*" >mid_list


uid_num=`wc -l "./uid_list" | awk '{print $1}'`
if [ $uid_num -lt 10000 ] ;then
	echo "uid_num less 10000"
	alert "ALS日志生成问题"
	exit
fi
