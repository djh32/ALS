package datamining.als;
// 导入时间类

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import java.lang.Math;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.mapred.Reducer;


public class AlsUidMidScore
{
  private static enum Counter
  {
    PLAY_ACTION_NUM,  OTHER_ACTION_NUM,  PLAY_RECORD_NUM,  REPOST_RECORD_NUM,  ATTITUDE_RECORD_NUM,  COMMENT_RECORD_NUM,  UID_NUM,  MID_BY_ONE_UID,  ACTION_LESS_60_USER_NUM,  ACTION_LARGE_70000_USER_NUM;
    
    private Counter() {}
  }
  
  public static class BehaviourDataMapper<K extends LongWritable, V extends Text>
    extends MapReduceBase
    implements Mapper<K, V, BytesWritable, BytesWritable>
  {
    private Set<String> mid_set = new HashSet();
    
    public String getPlayInfo(String extend)
    {
      try
      {
        Map<String, String> extendMap = new HashMap();
        String[] split = extend.split("\\|");
        for (String string : split)
        {
          String[] split2 = string.split(":");
          String mapKey = split2[0];
          String mapValue = split2[1];
          extendMap.put(mapKey, mapValue);
        }
        double object_duration = Double.parseDouble((String)extendMap.get("object_duration"));
        double duration = Double.parseDouble((String)extendMap.get("valid_play_duration"));
        if (duration < 3000.0D) {
          return null;
        }
        double rate = duration / object_duration;
        
        int deep = 0;
        if (extendMap.get("index") == null) {
          deep = 1;
        } else {
          deep = Integer.parseInt((String)extendMap.get("index"));
        }
        if (deep == 0) {
          return "daoliu_" + rate + "_" + object_duration;
        }
        return "play_" + rate + "_" + object_duration;
      }
      catch (Exception e)
      {
        System.err.println(extend);
        System.err.println("error in getPlayInfo: " + e);
      }
      return "";
    }
    
    public void configure(JobConf job)
    {
      String input_file = job.get("midfile");
      try
      {
        BufferedReader rdr = new BufferedReader(new FileReader(input_file));
        String line_str = null;
        while ((line_str = rdr.readLine()) != null) {
          mid_set.add(line_str.trim());
        }
        rdr.close();
      }
      catch (Exception e)
      {
        System.err.println("error in UserInterestMapper configure: " + e);
      }
    }
    
    public void map(K key, V value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
      throws IOException
    {
      try
      {
        String value_str = new String(value.getBytes(), 0, value.getLength(), "UTF-8");
        String[] value_pair = value_str.split("\t");
        String uid = value_pair[0];
        if (uid.length() > 11) {
          return;
        }
        String action = value_pair[1];
        String mid = value_pair[3];
        if (!mid_set.contains(mid)) {
          return;
        }
        if (action.equals("play"))
        {
          String extendStr = value_pair[5];
          String outPut = mid + "_" + getPlayInfo(extendStr);
          reporter.incrCounter(AlsUidMidScore.Counter.PLAY_ACTION_NUM, 1L);
          output.collect(new BytesWritable(uid.getBytes()), new BytesWritable(outPut.getBytes()));
        }
        else
        {
          reporter.incrCounter(AlsUidMidScore.Counter.OTHER_ACTION_NUM, 1L);
          output.collect(new BytesWritable(uid.getBytes()), new BytesWritable((mid + "_" + action).getBytes()));
        }
      }
      catch (Exception e)
      {
        System.err.println("error in BehaviourDataMapper: " + e);
      }
    }
  }
  
  public static class MyReducer<K extends BytesWritable, V extends BytesWritable> extends MapReduceBase implements Reducer<K, V, Text, Text>
  {
    class ScoreType
    {
      Set<Double> play_rate_set = new HashSet();
      Set<Double> daoliu_rate_set = new HashSet();
      double attitude = 0.0D;
      double repost = 0.0D;
      double comment = 0.0D;
      double obj_duration = 0.0D;
      
    }
    
    public double Calcu(ScoreType st)
    {
      int play_time = st.play_rate_set.size();
      int daoliu_time = st.daoliu_rate_set.size();
      double aver_play_rate = 0.0D;
      double aver_daoliu_rate = 0.0D;
      double rtn_score = 0.0D;
      double sum = 0.0D;
      Iterator localIterator;
      if (daoliu_time != 0)
      {
        double max_daoliu_rate = 0.0D;
        for (localIterator = st.daoliu_rate_set.iterator(); localIterator.hasNext();)
        {
          double i = ((Double)localIterator.next()).doubleValue();
          if (i > max_daoliu_rate) {
            max_daoliu_rate = i;
          }
          sum += i;
        }
       // if (daoliu_time == 1) {
       //   if (max_daoliu_rate > 0.8D) {   		//一次大雨80的导流 得分小于点赞 大于评论
       //     rtn_score += 2.0D;
       //   } else if (max_daoliu_rate > 0.5) {		// 一次 30-80 之间的导流 得分等于评论
       //     rtn_score += 1.0;
       //   } 
       //   /*else {
       //     rtn_score += 5.0D;				//小于30的 等于一次完整观看
       //   }*/
       // }
       // if (daoliu_time == 2) {
       //   rtn_score += 3.0D;
       // }
       // if (daoliu_time >= 3) {
       //   rtn_score += 4.0D;
       // }
         if(max_daoliu_rate>1.0) max_daoliu_rate=1.0;
	// if(daoliu_time == 1)
	 //{ 
	 rtn_score += daoliu_time * max_daoliu_rate * 2.0;
	 //}
	 if ((daoliu_time == 2) && (max_daoliu_rate> 0.8)) rtn_score += 5;    //重复观看
	 if ((daoliu_time >2 )&& (max_daoliu_rate> 0.8)) rtn_score += 16;
      }
      sum = 0.0D;
      if (play_time != 0)
      {
        double max_play_rate = 0.0D;
        for (localIterator = st.play_rate_set.iterator(); localIterator.hasNext();)
        {
          double i = ((Double)localIterator.next()).doubleValue();
          if (i > max_play_rate) {
            max_play_rate = i;
          }
          sum += i;
        }
        //  if (max_play_rate > 0) {     // 一次观看完成率小于90 则+5分
        //    rtn_score += play_time * max_play_rate ;
        //  } 
       //   //else if (max_play_rate > 0.5D) {        //一次观看小于50 则+2分
       //   //  rtn_score += 2.0  ;
       //   //}
       // }
       // //if (play_time == 2) {    		//2次观看不考虑完成率  都算7分 说明又拉回来看了
       // //  rtn_score += 2.0 * max_play_rate;
       // //}
         if(max_play_rate> 1.0) max_play_rate=1.0;
         //if(play_time == 1){
	 rtn_score += play_time * max_play_rate ;
	 //}
	 if ((play_time ==2 ) && (max_play_rate > 0.8)) rtn_score += 5;    //重复观看
	 if ((play_time > 2 ) && (max_play_rate > 0.8)) rtn_score += 16;    //重复观看
      }

      rtn_score += st.comment * 4.0D + st.attitude * 16.0D + st.repost * 20.0D;
      return rtn_score;
    }
    
    public void reduce(K key, Iterator<V> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
    {
      try
      {
        String uid = new String(key.getBytes(), 0, key.getLength());
        reporter.incrCounter(AlsUidMidScore.Counter.UID_NUM, 1);
        HashMap<String, ScoreType> midType = new HashMap();
        HashMap<String, Double> midScore = new HashMap();
        while (values.hasNext())
        {
	  BytesWritable value = values.next();
          String value_str = new String(value.getBytes(), 0, value.getLength());
          String[] midList = value_str.split("_");
          String mid = midList[0];
          String act = midList[1];
          ScoreType midAct;
          if (midType.get(mid) == null) {
            midAct = new ScoreType();
          } else {
            midAct = (ScoreType)midType.get(mid);
          }
          double rate = 0.0D;
          switch (act)
          {
          case "daoliu": 
            rate = Double.parseDouble(midList[2]);
            midAct.daoliu_rate_set.add(Double.valueOf(rate));
            midAct.obj_duration = Double.parseDouble(midList[3]);
            reporter.incrCounter(AlsUidMidScore.Counter.PLAY_RECORD_NUM, 1L);
            break;
          case "play": 
            rate = Double.parseDouble(midList[2]);
            midAct.play_rate_set.add(Double.valueOf(rate));
            midAct.obj_duration = Double.parseDouble(midList[3]);
            reporter.incrCounter(AlsUidMidScore.Counter.PLAY_RECORD_NUM, 1L);
            break;
          case "repost": 
            midAct.repost = 1.0D;
            reporter.incrCounter(AlsUidMidScore.Counter.REPOST_RECORD_NUM, 1L);
            break;
          case "comment": 
            midAct.comment = 1.0D;
            reporter.incrCounter(AlsUidMidScore.Counter.COMMENT_RECORD_NUM, 1L);
            break;
          case "attitude": 
            midAct.attitude = 1.0D;
            reporter.incrCounter(AlsUidMidScore.Counter.ATTITUDE_RECORD_NUM, 1L);
          }
          midType.put(mid, midAct);
        }
        if (midType.size() < 50)
        {
          reporter.incrCounter(AlsUidMidScore.Counter.ACTION_LESS_60_USER_NUM, 1L);
          return;
        }
        if (midType.size() > 2000)
        {
          reporter.incrCounter(AlsUidMidScore.Counter.ACTION_LARGE_70000_USER_NUM, 1L);
          return;
        }
        for (Map.Entry<String, ScoreType> entry : midType.entrySet())
        {
          double score = Calcu(entry.getValue());
          if (score != 0.0D)
          {
            String out = (String)entry.getKey() + "\t" + score;
            reporter.incrCounter(AlsUidMidScore.Counter.MID_BY_ONE_UID, 1L);
            output.collect(new Text(uid), new Text(out));
          }
        }
      }
      catch (Exception e)
      {
        BytesWritable value;
        System.err.println("error in MyReducer: " + e);
      }
    }
  }
  
  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3)
    {
      System.out.println("Usage: <action_source> <result> <reduce_num>" + otherArgs.length);
      System.exit(0);
    }
    JobConf job = new JobConf(conf);
    job.setJobName("AlsUidMidScore");
    job.setJarByClass(AlsUidMidScore.class);
    



    String[] input_recom_files = otherArgs[0].split(",");
    for (int i = 0; i < input_recom_files.length; i++) {
      MultipleInputs.addInputPath(job, new Path(input_recom_files[i]), TextInputFormat.class, BehaviourDataMapper.class);
    }
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);
    

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    job.setReducerClass(MyReducer.class);
    
    job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
    
    JobClient.runJob(job);
  }
}
