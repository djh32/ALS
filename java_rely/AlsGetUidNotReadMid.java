package datamining.als;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;

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

public class AlsGetUidNotReadMid{

  private static enum Counter
  {
    NOT_IN_MATERIAL_ACTION_NUM,  PLAY_RECORD_NUM,  REPOST_RECORD_NUM,  ATTITUDE_RECORD_NUM,  COMMENT_RECORD_NUM,  UID_NUM,  MID_BY_ONE_UID,  ACTION_LESS_60_USER_NUM,  ACTION_LARGE_70000_USER_NUM; }
  
  public static class ScoreMapper<K extends LongWritable, V extends Text>
    extends MapReduceBase
    implements Mapper<K, V, BytesWritable, BytesWritable>
  {
    public void map(K key, V value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
      throws IOException
    {
      try
      {
        String value_str = new String(value.getBytes(), 0, value.getLength(), "UTF-8");
        String[] value_pair = value_str.split("\t");
        
        String uid = value_pair[0];
        String mid = value_pair[1];
        output.collect(new BytesWritable(uid.getBytes()), new BytesWritable((mid + "_look").getBytes()));
      }
      catch (Exception e)
      {
        System.err.println("error in BehaviourDataMapper: " + e);
      }
    }
  }
  
  public static class recomMapper<K extends LongWritable, V extends Text>
    extends MapReduceBase
    implements Mapper<K, V, BytesWritable, BytesWritable>
  {
    public void map(K key, V value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
      throws IOException
    {
      try
      {
        String value_str = new String(value.getBytes(), 0, value.getLength(), "UTF-8");
        String[] value_pair = value_str.split("_");
        
        String uid = value_pair[0];
        String[] mid_list = value_pair[1].split(",");
        for (int index = 0; index < mid_list.length; index++)
        {
          String out_val = mid_list[index] + "_" + index;
          output.collect(new BytesWritable(uid.getBytes()), new BytesWritable(out_val.getBytes()));
        }
      }
      catch (Exception e)
      {
        System.err.println("error in BehaviourDataMapper: " + e);
      }
    }
  }
  
  public static class MyReducer<K extends BytesWritable, V extends BytesWritable>
    extends MapReduceBase
    implements Reducer<K, V, Text, Text>
  {
    public void reduce(K key, Iterator<V> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
    {
      try
      {
        String uid = new String(key.getBytes(), 0, key.getLength());
        reporter.incrCounter(AlsGetUidNotReadMid.Counter.UID_NUM, 1L);
        HashMap<String, Integer> index_mid = new HashMap();
        HashSet<String> lookedMid = new HashSet();
        String value_str;
        String mid;
        while (values.hasNext())
        {
          BytesWritable value = (BytesWritable)values.next();
          value_str = new String(value.getBytes(), 0, value.getLength());
          String[] li = value_str.split("_");
          mid = li[0];
          String index = li[1];
          if (!index.equals("look")) {
            index_mid.put(mid, Integer.valueOf(Integer.parseInt(index)));
          } else {
            lookedMid.add(mid);
          }
        }
        HashMap<String, Integer> out_mid = new HashMap();
        for (Map.Entry<String, Integer> temp : index_mid.entrySet()) {
          if (!lookedMid.contains(temp.getKey())) {
            out_mid.put(temp.getKey(), temp.getValue());
          }
        }
        if (out_mid.size() == 0) {
          return;
        }
        List<Map.Entry<String, Integer>> list = new ArrayList(out_mid.entrySet());
	Collections.sort(list,new Comparator<Map.Entry<String,Integer>>() {
			    public int compare(Map.Entry<String, Integer> o1,Map.Entry<String, Integer> o2) {
				    return o1.getValue().compareTo(o2.getValue());
			    }
		    });
        List<String> result = new ArrayList();
        for (Map.Entry<String, Integer> temp : list) {
          result.add(temp.getKey());
        }
        String out = StringUtils.join(result.toArray(), ",");
        output.collect(new Text(uid), new Text(out));
      }
      catch (Exception e)
      {
        System.err.println("error in MyReducer: " + e);
      }
    }
  }
  
  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4)
    {
      System.out.println("Usage: <action_source> <result> <reduce_num>" + otherArgs.length);
      System.exit(0);
    }
    JobConf job = new JobConf(conf);
    job.setJobName("AlsGetUidNotReadMid");
    job.setJarByClass(AlsGetUidNotReadMid.class);
    



    String[] input_recom_files = otherArgs[0].split(",");
    for (int i = 0; i < input_recom_files.length; i++) {
      MultipleInputs.addInputPath(job, new Path(input_recom_files[i]), TextInputFormat.class, ScoreMapper.class);
    }
    String[] input = otherArgs[1].split(",");
    for (int i = 0; i < input.length; i++) {
      MultipleInputs.addInputPath(job, new Path(input[i]), TextInputFormat.class, recomMapper.class);
    }
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);
    

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    job.setReducerClass(MyReducer.class);
    
    job.setNumReduceTasks(Integer.parseInt(otherArgs[3]));
    
    JobClient.runJob(job);
  }
}
