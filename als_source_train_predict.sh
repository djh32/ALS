#!/usr/bin/bash
. ~/.bash_profile



main(){

  all_bg=`date +%s`
  calcu_day=`date -d "1 days ago" +%Y%m%d`
  delete_day=`date -d "5 days ago" +%Y%m%d`
  echo "calcu day is :$calcu_day"
  hadoop fs -mkdir /user_ext/jianhang3/ALS_short_video/$calcu_day/
  hadoop fs -rmr /user_ext/jianhang3/ALS_short_video/$delete_day


  begin=`date +%s`
  cd mr_get_source
  sh get_uid_mid_score_top.sh $calcu_day> log 2>&1      #热门mid
  #sh get_uid_mid_score.sh $calcu_day> log 2>&1      #不热门
  cd ..
  end1=`date +%s`
  echo "数据制备用时:"$(($end1 - $begin))
  
  begin=`date +%s`
  cd train_predict
  sh train_model.sh $calcu_day>train_log 2>&1
  cd ..
  end1=`date +%s`
  echo "训练模型用时:"$(($end1 - $begin))

  begin=`date +%s`
  cd distribute_id
  sh get_KMeans.sh $calcu_day > part_log 2>&1
  cd ..
  end1=`date +%s`
  echo "划分数据用时:"$(($end1 - $begin))

  begin=`date +%s`
  cd train_predict
  sh multiple_als_jianhang_time.sh $calcu_day > predict_log 2>&1 
  cd ..
  end1=`date +%s`
  echo "用户预测用时:"$(($end1 - $begin))

  begin=`date +%s`
  cd remove_looked_mid_total
  sh remove_looked_mid_total.sh $calcu_day > remove_looked_log 2>&1
  cd ..
  end1=`date +%s`
  echo "已读过滤用时:"$(($end1 - $begin))

  begin=`date +%s`
  cd get_mid_info
  sh get_mid_info.sh $calcu_day > info_log 2>&1
  cd ..
  end1=`date +%s`
  echo "获取候选mid信息用时:"$(($end1 - $begin))

  begin=`date +%s`
  cd base_load
  sh data_base_merge.sh $calcu_day > update_log 2>&1
  cd ..
  end1=`date +%s`
  echo "数据库更新用时:"$(($end1 - $begin))

  begin=`date +%s`
  cd write_to_lushan
  sh up_to_lushan.sh $calcu_day > up_to_lushan_log 2>&1
  cd ..
  end1=`date +%s`
  result_num=`grep 'RESULT' write_to_lushan/up_to_lushan_log |awk -F '=' '{print $2}'`
  echo "庐山上传用时:"$(($end1 - $begin))

  echo "用户结果数量:${result_num}"
  all_finish=`date +%s`
  echo "ALS 计算总用时:"$(($all_finish - $all_bg))
}

main

