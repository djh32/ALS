package datamining.als;

// 导入时间类
import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reducer;

import com.weibo.r9.LushanFileOutputFormat;
import com.weibo.r9.PlainTextWritable;

/*
 * 将TextFile格式转换成lushan格式
 * */
public class AlsLushanTextTo 
{
    static final String NAME = "AlsLushanTextTo";

    private static enum Counter { 
        MAP_NUM,
        RESULT_NUM
    }

    public static class InputMap<K extends LongWritable, V extends Text>
            extends MapReduceBase implements Mapper<K, V, LongWritable, Text> {

        @Override
        public void map(K key, V value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            try{
                String value_str = new String(value.getBytes(),0,value.getLength(),"UTF-8");
                String[] value_pair = value_str.split("\t");
                String uid_str   = value_pair[0];
                String items_str = value_pair[1];
		if (items_str.equals("")) return;
                long uid = Long.parseLong(uid_str);

                output.collect(new LongWritable(uid), new Text(items_str));
                reporter.incrCounter(Counter.MAP_NUM, 1);

            } catch (Exception e) {
                System.err.println("error in InputMap: " + e); 
            }
        }
    }

    public static class MyReducer<K extends LongWritable, V extends Text> extends
        MapReduceBase implements Reducer<K, V, LongWritable, PlainTextWritable> {
            
            public void reduce(K key, Iterator<V> values, OutputCollector<LongWritable, PlainTextWritable> output, Reporter reporter) throws IOException{
                try{
                    // 获取uid
                    long uid = key.get();

                    while (values.hasNext()) {
                        Text value_text = values.next();
                        String value_str = new String(value_text.getBytes(), 0, value_text.getLength(), "UTF-8");
                        if(value_str.length() > 1){
                            output.collect(new LongWritable(uid), new PlainTextWritable(value_str));
                            reporter.incrCounter(Counter.RESULT_NUM, 1);
                        }
                    }

                }catch (Exception e) {
                    System.err.println("error in MyReducer: " + e);
                }
            }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.out.println("Usage: <input> <output>" + otherArgs.length);
            System.exit(0);
        }

        JobConf job = new JobConf(conf);
        job.setJobName("AlsLushanTextTo");
        job.setJarByClass(AlsLushanTextTo.class);   
     
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
            TextInputFormat.class, datamining.als.AlsLushanTextTo.InputMap.class);

        // 设置MAP输出的key和value的格式
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormat(LushanFileOutputFormat.class);
        job.setPartitionerClass(com.weibo.r9.LongWritableModPartitioner.class);
        job.setReducerClass(MyReducer.class);
        
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
       
        job.setNumReduceTasks(8);

        JobClient.runJob(job);
    }
} 

