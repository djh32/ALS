package datamining.als;
// 导入时间类

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
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
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.mapred.Reducer;

public class AlsGetCalcuId{

	private static enum Counter {
	NOT_IN_MATERIAL_ACTION_NUM,PLAY_RECORD_NUM,REPOST_RECORD_NUM,ATTITUDE_RECORD_NUM, COMMENT_RECORD_NUM,UID_NUM, RECOM_MID_NUM_IN_MATERIAL
	}
    // 行为数据
    public static class BehaviourDataMapper<K extends LongWritable, V extends Text>
            extends MapReduceBase implements Mapper<K, V, BytesWritable, BytesWritable> {
        @Override
        public void map(K key, V value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter) throws IOException {
            try{
                String value_str = new String(value.getBytes(),0,value.getLength(),"UTF-8");
                String[] value_pair = value_str.split("\t");
		String uid = value_pair[0];
		String mid = value_pair[1];
		String out_key = uid.substring(uid.length() - 4);
		if(out_key.charAt(0)=='0'){
			out_key = new StringBuilder(out_key).replace(0,1,"1").toString();
		}
		String out_val = uid + "_uid";
		output.collect(new BytesWritable(out_key.getBytes()), new BytesWritable(out_val.getBytes()));
		out_key = mid.substring(mid.length() - 4);
		if(out_key.charAt(0)=='0'){
			out_key = new StringBuilder(out_key).replace(0,1,"1").toString();
		}
		out_val = mid + "_mid";
		output.collect(new BytesWritable(out_key.getBytes()), new BytesWritable(out_val.getBytes()));

            } catch (Exception e) {
                System.err.println("error in BehaviourDataMapper: " + e); 
            }
        }
    }

    public static class MyReducer<K extends BytesWritable, V extends BytesWritable> extends
        MapReduceBase implements Reducer<K, V, Text, Text> {
		@Override
            public void reduce(K key, Iterator<V> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
                try{
                    // 获取uid
                    String key_val =new String(key.getBytes(), 0, key.getLength());;
		    reporter.incrCounter(Counter.UID_NUM,1);
		    HashSet<String> uid_set = new HashSet<String>();
		    HashSet<String> mid_set = new HashSet<String>();
                    while (values.hasNext()) {
			BytesWritable value = values.next();
                        String value_str = new String(value.getBytes(), 0, value.getLength());
			String []list = value_str.split("_");
			if (list[1].equals("mid")){
				mid_set.add(list[0]);
			}
			if(list[1].equals("uid")){
				uid_set.add(list[0]);
			}	
		    } //whilte
			
		    int id = 0;
		    for (String uid: uid_set){
			String out_val = key_val+id+"_uid";        //  String + int = string    "123" + 1 =1231
			id += 1;
		   	output.collect(new Text(uid), new Text(out_val ) ); 
		    }
		    id = 0;
		    for (String mid: mid_set){
			String out_val = key_val+id+"_mid";        //  String + int = string    "123" + 1 =1231
			id += 1;
		   	output.collect(new Text(mid), new Text(out_val ) ); 
		    }
            
                }catch (Exception e) {
                    System.err.println("error in MyReducer: " + e);
                }
    	}
    // 支持多目录输出
} //reduce

public static class MyMultipleOutputFormat extends MultipleOutputFormat <Text, Text>{
 		
 		@Override
      protected RecordWriter<Text, Text> getBaseRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException{
	  //final SequenceFileOutputFormat<Text, Text> of = new SequenceFileOutputFormat<Text, Text>();
          final TextOutputFormat<Text, Text> of = new TextOutputFormat<Text, Text>();
          return of.getRecordWriter(fileSystem,jobConf,s,progressable);
      }

 		protected String generateFileNameForKeyValue(Text key, Text value, String name) {  
 		try {
// 			 String storyid_score = new String(value.getBytes(), 0, value.getLength());
// 			 String sid = storyid_score.split("_")[0];
// 			 String tail_num=sid.substring(sid.length()-2,sid.length()-1);
// 			 
// 			 return tail_num+"/"+name;
//
			 String value_str =  new String(value.getBytes(),0,value.getLength(),"UTF-8");
 			 String out_dir = value_str.split("_")[1];

			 System.err.println(value_str);
			 System.err.println(out_dir);

			 return out_dir+"/"+name;
 			}
 		catch (Exception e) {
 				System.err.println("err in generateFileNameForKeyValue" + e);
 			}
		return "err";

      } //generateFileNameForKeyValue
  } //MyMultipleOutputFormat
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.out.println("Usage: <action_source> <result> <reduce_num>" + otherArgs.length);
            System.exit(0);
        }

        JobConf job = new JobConf(conf);
        job.setJobName("AlsGetCalcuId");
        job.setJarByClass(AlsGetCalcuId.class);   
     
        //MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
        //    TextInputFormat.class, datamining.als.AlsGetCalcuId.BehaviourDataMapper.class);

	String[] input_recom_files = otherArgs[0].split(",");
	for (int i = 0; i < input_recom_files.length; i++) {
		MultipleInputs.addInputPath(job, new Path(input_recom_files[i]), TextInputFormat.class,
				datamining.als.AlsGetCalcuId.BehaviourDataMapper.class);
	}

        // 设置MAP输出的key和value的格式
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        
        // 设置REDUCE输出的key和value的格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormat(MyMultipleOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));        
        job.setReducerClass(MyReducer.class);

        job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));

        JobClient.runJob(job);
    }
} 

