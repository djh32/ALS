#!usr/bin/bash



function filt_string(){
root_str=$1

o_s=""
for i in {1..7}
do
	t_d=`date -d "${i} days ago" +%Y%m%d`
	#echo "${root_str}/${t_d}/part"
	f=`hadoop fs -test -e "${root_str}/dt=${t_d}/part-00000"`
	if [ $? -eq 0 ];then
	    o_s="${o_s}""${root_str}/${t_d}/*,"
	fi
	#echo ${o_s}
done

echo ${o_s}
}

s=`filt_string "/user_ext/weibo_recmd/warehouse/little_video_behaviour"`
echo "sss:${s}"

