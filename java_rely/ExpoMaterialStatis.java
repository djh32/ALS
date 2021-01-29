package statistics;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.io.IOException;
import java.lang.Math;
import java.math.BigDecimal;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;

import breeze.stats.hist;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 
 * 统计竖版视频流推荐日报 基础消费数据表中的 导流位 ：消费次数、消费人数、消费时长
 * 推荐位（整体：曝光次数、曝光人数、消费次数、消费人数、消费时长、人均消费次数、人均消费时长、单次消费时长 推荐位（第1位）消费次数、消费人数
 *
 * history:2018/4/25
 * 		   2018/5/16
 * 
 * @author zexing
 * 
 */
public class ExpoMaterialStatis {

	private static enum Counter {
		RECORD_NUM, NO_UICODE, NO_EXTEND, INVALID_RECORD_NUM, RESULT_NUM // 最终结果数量
		, NO_10000756_UICODE, INVALID_PLAY_DURATION_RECORD_NUM, NO_PLAY_DURATION_RECORD_NUM, index0_RECORD_NUM, index1_RECORD_NUM, NO_799_OR_004_ACTION, YOUKE_UID_NUM, NO_UID, NO_799_ACTION, TWO_PAGE_RECORD_NUM, ISAUTOPLAY_RECORD_NUM, NO_ISAUTOPLAY_RECORD_NUM, NO_FROM_VAL, IS_1084393010_FROM_VAL, NO_PAGE_Index_RECORD_NUM, NO_RECOM_CODE_RECORD_NUM, VAILD_RECOM_CODE_RECORD_NUM, IS_recom_scene1_RECORD_NUM,NO_INDEX_OR_PAGE_RECORD_NUM,DEEP_IS_ZERO,NO_FROM,BAD_AUTO_PLAY

	}

	// 行为
	public static class videoStreamMapper<K extends LongWritable, V extends BytesRefArrayWritable>
			extends MapReduceBase implements Mapper<K, V, BytesWritable, BytesWritable> {

		@Override
		public void map(K key, V value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
				throws IOException {
			try {
				reporter.incrCounter(Counter.RECORD_NUM, 1);

				
				// 提取recom_list
				BytesRefWritable recom_list_rc = value.get(9);
				String recom_list_str = new String(recom_list_rc.getData(), recom_list_rc.getStart(), recom_list_rc.getLength(), "UTF-8");
				String recom_list [] = recom_list_str.trim().split(",");

				//提取type
				BytesRefWritable type_rc = value.get(8);
				String type = new String(type_rc.getData(), type_rc.getStart(), type_rc.getLength(), "UTF-8");
				
				if (!type.equals("discover")) return;
				for (int i=0; i<recom_list.length; i++){
					String []info_list = recom_list[i].trim().split("\\|");
					String mid = info_list[0];
					String auth_id = info_list[1];
					output.collect(new BytesWritable( auth_id.getBytes() ), new BytesWritable( mid.getBytes()  )   );
				}
				
				//4275788172008670|3490456653|1034:4275788128661737|3490456653|4275788172008670|4001|1|1|0|1034:4276116987259825|0,4275051622898852|2640607485|1034:4275051592411824|2640607485|null|4001|1|2|0|1034:4276116987259825|0,427450351963

				return;

			} catch (Exception e) {
				System.err.println("error in storyBehaviourMapper: " + e);
			}
		}
	}

	public static class MyReducer<K extends BytesWritable, V extends BytesWritable> extends MapReduceBase
			implements Reducer<K, V, Text, Text> {
		

		public void reduce(K key, Iterator<V> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {
				HashSet<String> mid_set = new HashSet<String>();
				// 获取type
				String outKey = new String(key.getBytes(), 0, key.getLength());
				while (values.hasNext()) {
					BytesWritable value = values.next();
					String value_str = new String(value.getBytes(), 0, value.getLength());
					mid_set.add(value_str);
				}
				String out_val = ""+mid_set.size();
				output.collect(new Text(outKey ), new Text( out_val));
						
			} catch (Exception e) {
				System.err.println("error in MyReducer: " + e);
				
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.out.println("Usage: <behaviour> <output> <reducenum> " + otherArgs.length);
			System.exit(0);
		}

		JobConf job = new JobConf(conf);
		job.setJobName("ExpoMaterialStatisTab");
		job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
		job.setJarByClass(ExpoMaterialStatis.class);

		String[] input_recom_files = otherArgs[0].split(",");
		for (int i = 0; i < input_recom_files.length; i++) {
			MultipleInputs.addInputPath(job, new Path(input_recom_files[i]), RCFileInputFormat.class,
					statistics.ExpoMaterialStatis.videoStreamMapper.class);
		}

		job.setReducerClass(MyReducer.class);

		// 设置MAP输出的key和value的格式
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);

		// 设置REDUCE输出的key和value的格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		JobClient.runJob(job);
	}



	
	/**
	 * 提供（相对）精确的除法运算。当发生除不尽的情况时，由scale参数指 定精度，以后的数字四舍五入。
	 * 
	 * @param v1
	 *            被除数
	 * @param v2
	 *            除数
	 * @param scale
	 *            表示需要精确到小数点以后几位。
	 * @return 两个参数的商
	 */
	public static double div(long v1, long v2, int scale) {
		if (scale < 0) {
			throw new IllegalArgumentException("The scale must be a positive integer or zero");
		}
		BigDecimal b1 = new BigDecimal(Double.toString((double) v1));
		BigDecimal b2 = new BigDecimal(Double.toString((double) v2));
		return b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	/**
	 * 判断是否为整数
	 * 
	 * @param str
	 *            传入的字符串
	 * @return 是整数返回true,否则返回false
	 */
	public static boolean isInteger(String str) {
		Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
		return pattern.matcher(str).matches();
	}

}
