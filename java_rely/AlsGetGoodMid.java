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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
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

public class AlsGetGoodMid{

	private static enum Counter {
	MATERIAL_ACTION_NUM,PLAY_RECORD_NUM,REPOST_RECORD_NUM,ATTITUDE_RECORD_NUM, COMMENT_RECORD_NUM,UID_NUM, MID_BY_ONE_UID,ACTION_LESS_60_USER_NUM,ACTION_LARGE_70000_USER_NUM,HOT_PASS_MID_NUM,ALL_MID_NUM 
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
		if (uid.length()>11) return;
		String action = value_pair[1];
		String mid = value_pair[3];
		reporter.incrCounter(Counter.MATERIAL_ACTION_NUM,1);
		output.collect(new BytesWritable(mid.getBytes()), new BytesWritable(action.getBytes()));

            } catch (Exception e) {
                System.err.println("error in BehaviourDataMapper: " + e); 
            }
        }
    }

    public static class MyReducer<K extends BytesWritable, V extends BytesWritable> extends
        MapReduceBase implements Reducer<K, V, Text, NullWritable> {
		@Override
            public void reduce(K key, Iterator<V> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException{
		double play=0.0f;
		double repost = 0.0f;
		double attitude = 0.0f;
		double comment = 0.0f;
		double action_good = 0.0f;
                try{
                    String mid =new String(key.getBytes(), 0, key.getLength());;
		    reporter.incrCounter(Counter.ALL_MID_NUM,1);
                    while (values.hasNext()) {
			BytesWritable value = values.next();
                        String action = new String(value.getBytes(), 0, value.getLength());
			if (action.equals("play")) play +=1;
			else if (action.equals("repost")) repost+=1;
			else if (action.equals("attitude")) attitude+=1;
			else if (action.equals("comment")) comment+=1;
		    } //while

		//if(play >480554 ){
		// reporter.incrCounter(Counter.HOT_PASS_MID_NUM,1);      
		//	return;    //过掉大热  
		//}

		//if (play >2000  ){
		//	output.collect(new Text(mid), NullWritable.get());	
		//} 
		String result =mid+ "\t"+repost+"\t"+comment+"\t"+attitude+"\t"+play;
		output.collect(new Text(result), NullWritable.get());
	    }
	    catch (Exception e) {
                    System.err.println("error in MyReducer: " + e);
                }
    	}
 /*   // 支持多目录输出
 	public static class MyMultipleOutputFormat extends MultipleOutputFormat<BytesWritable, BytesWritable>{
 		
 		@Override
      protected RecordWriter<BytesWritable, BytesWritable> 
 		getBaseRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException{
          final SequenceFileOutputFormat<BytesWritable, BytesWritable> of = new SequenceFileOutputFormat<BytesWritable, BytesWritable>();
          return of.getRecordWriter(fileSystem,jobConf,s,progressable);
      }

 		protected String generateFileNameForKeyValue(BytesWritable key, BytesWritable value, String name) {  
 		try {
 			 String storyid_score = new String(value.getBytes(), 0, value.getLength());
 			 String sid = storyid_score.split("_")[0];
 			 String tail_num=sid.substring(sid.length()-2,sid.length()-1);
 			 
 			 return tail_num+"/"+name;

 			}
 		catch (Exception e) {
 				System.err.println("err in generateFileNameForKeyValue" + e);;
 			}
		return "err";

      } //generateFileNameForKeyValue
  } //MyMultipleOutputFormat
  */
} //reduce

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.out.println("Usage: <action_source> <result> <reduce_num>" + otherArgs.length);
            System.exit(0);
        }

        JobConf job = new JobConf(conf);
        job.setJobName("AlsGetGoodMid");
        job.setJarByClass(AlsGetGoodMid.class);   
     
        //MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
        //    TextInputFormat.class, datamining.als.AlsGetGoodMid.BehaviourDataMapper.class);

	String[] input_recom_files = otherArgs[0].split(",");
	for (int i = 0; i < input_recom_files.length; i++) {
		MultipleInputs.addInputPath(job, new Path(input_recom_files[i]), TextInputFormat.class,
				datamining.als.AlsGetGoodMid.BehaviourDataMapper.class);
	}

        // 设置MAP输出的key和value的格式
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        
        // 设置REDUCE输出的key和value的格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));        
        job.setReducerClass(MyReducer.class);

        job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));

        JobClient.runJob(job);
    }
} 

