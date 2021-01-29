package datamining.als;
// 导入时间类

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

public class AlsDataBaseMerge{

	private static enum Counter {
	UID_NUM,NEW_CANDIDATE_UID_NUM,UPDATE_CANDIDATE_UID_NUM,DECREASE_CANDIDATE_UID_NUM,REMOVE_UID_NUM	
	}


    // 行为数据
    public static class NewCandicateMapper<K extends LongWritable, V extends Text>
            extends MapReduceBase implements Mapper<K, V, BytesWritable, BytesWritable> {
		public static String getExceptionAllinformation(Exception ex) {
			String sOut = "";
			StackTraceElement[] trace = ex.getStackTrace();
			for (StackTraceElement s : trace) {
				sOut += "\tat " + s + "\r\n";
			}
			return sOut;
		}
        @Override
        public void map(K key, V value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter) throws IOException {
            try{
                String value_str = new String(value.getBytes(),0,value.getLength(),"UTF-8");
                String[] value_pair = value_str.split("\t");     //uid    midlist
		String uid = value_pair[0];
		//String mid_all_candidate[] = value_pair[1].split(",");
		String mid_all_candidate = value_pair[1];
		
		List<String> result =new ArrayList<String>();
		//存在小于100候选的用户
		//int len = 0;
		//if(mid_all_candidate.length > 100) len = 100;
		//else{
		//	len = mid_all_candidate.length;
		//}
		//for (int i=0 ;i<len;i++){    //候选集数量， 改变可以调整最终结果大小，影响lushan数据量
		//	result.add(mid_all_candidate[i]);
		//}
		//String out = StringUtils.join(result.toArray(),",");
		String mid_list =  "new" + "\t" + mid_all_candidate;
		output.collect(new BytesWritable(uid.getBytes()), new BytesWritable(mid_list.getBytes()));

            } catch (Exception e) {
                System.err.println("error in NewCandicateMapper: " + e); 
		System.err.println(getExceptionAllinformation(e));
            }
        }
    }

    // 行为数据
    public static class DataBaseMapper<K extends LongWritable, V extends Text>
            extends MapReduceBase implements Mapper<K, V, BytesWritable, BytesWritable> {
		public static String getExceptionAllinformation(Exception ex) {
			String sOut = "";
			StackTraceElement[] trace = ex.getStackTrace();
			for (StackTraceElement s : trace) {
				sOut += "\tat " + s + "\r\n";
			}
			return sOut;
		}
        @Override
        public void map(K key, V value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter) throws IOException {
            try{
                String value_str = new String(value.getBytes(),0,value.getLength(),"UTF-8");
                String[] value_pair = value_str.split("\t");     //uid    midlist    deadtime
		String uid = value_pair[0];
		String baseMid = value_pair[1];
		String deadTime = value_pair[2];
		String outVal = "base" + "\t" + baseMid +"\t" + deadTime;
		output.collect(new BytesWritable(uid.getBytes()), new BytesWritable((outVal).getBytes()));

            } catch (Exception e) {
                System.err.println("error in DataBaseDataMapper: " + e); 
		System.err.println(getExceptionAllinformation(e));
            }
        }
    }

    public static class MyReducer<K extends BytesWritable, V extends BytesWritable> extends
        MapReduceBase implements Reducer<K, V, Text, Text> {
		public static String getExceptionAllinformation(Exception ex) {
			String sOut = "";
			StackTraceElement[] trace = ex.getStackTrace();
			for (StackTraceElement s : trace) {
				sOut += "\tat " + s + "\r\n";
			}
			return sOut;
		}
		@Override
            public void reduce(K key, Iterator<V> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
                try{
                    // 获取uid
		    
                    String uid =new String(key.getBytes(), 0, key.getLength());;
		    reporter.incrCounter(Counter.UID_NUM,1);
		    String deadTime = "30";
		    String newCandi = "";
		    String baseCandi = "";
		    String baseDeadTime = "";
		    while (values.hasNext()) {
			BytesWritable value = values.next();
                        String value_str = new String(value.getBytes(), 0, value.getLength());
			String []val_list = value_str.split("\t");
			if (val_list[0].equals("new")) newCandi = val_list[1];
			if (val_list[0].equals("base")) {
				baseCandi = val_list[1];
				baseDeadTime = val_list[2];	
			}
		    } //while

		    //System.err.println("xin:"+newCandi);
		    //System.err.println("jiu:"+baseCandi);

		    if( (!newCandi.equals("")) && baseCandi.equals("")){
		    	//有新候选，无原始候选  赋予deadtime;
			reporter.incrCounter(Counter.NEW_CANDIDATE_UID_NUM,1);
			String out_val = newCandi + "\t" + deadTime;
			output.collect(new Text(uid),new Text(out_val));
		    } 
		    else if( (!newCandi.equals("")) && (!baseCandi.equals("")) ){
		    	//有新候选， 有原始候选，更新候选， 赋予deadtime;
			reporter.incrCounter(Counter.UPDATE_CANDIDATE_UID_NUM,1);
			String out_val = newCandi + "\t" + deadTime;
			output.collect(new Text(uid),new Text(out_val));
		    }
		    else if ( newCandi.equals("") && (!baseCandi.equals("")) ){
		    	//没有新候选， 旧候选有效time - 1 如果归零 则return;
			reporter.incrCounter(Counter.DECREASE_CANDIDATE_UID_NUM,1);
			int time = Integer.parseInt(baseDeadTime) -1;
			if (time == 0) {
				reporter.incrCounter(Counter.REMOVE_UID_NUM,1);
				return;
			}
			String out_val = baseCandi + "\t" + time;
			output.collect(new Text(uid),new Text(out_val));
		    }
                }catch (Exception e) {
                    System.err.println("error in MyReducer: " + e);
		     System.err.println(getExceptionAllinformation(e));
                }
    	}
} //reduce

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 4) {
            System.out.println("Usage: <action_source> <result> <reduce_num>" + otherArgs.length);
            System.exit(0);
        }

        JobConf job = new JobConf(conf);
        job.setJobName("AlsDataBaseMerge");
        job.setJarByClass(AlsDataBaseMerge.class);   
     
        //MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
        //    TextInputFormat.class, datamining.als.AlsDataBaseMerge.BehaviourDataMapper.class);

	String[] input_recom_files = otherArgs[0].split(",");
	for (int i = 0; i < input_recom_files.length; i++) {
		MultipleInputs.addInputPath(job, new Path(input_recom_files[i]), TextInputFormat.class,
				datamining.als.AlsDataBaseMerge.NewCandicateMapper.class);
	}

	String[] input = otherArgs[1].split(",");
	for (int i = 0; i < input.length; i++) {
		MultipleInputs.addInputPath(job, new Path(input[i]), TextInputFormat.class,
				datamining.als.AlsDataBaseMerge.DataBaseMapper.class);
	}
        // 设置MAP输出的key和value的格式
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        
        // 设置REDUCE输出的key和value的格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));        
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(Integer.parseInt(otherArgs[3]));
        JobClient.runJob(job);
    }
} 

