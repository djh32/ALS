package statistics.tab;

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
import org.apache.hadoop.io.NullWritable;
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
public class TabConsumeUserIos {
	static final String NAME = "VideoStream";

	private static enum Counter {
		RECORD_NUM, NO_UICODE, NO_EXTEND, INVALID_RECORD_NUM, RESULT_NUM // 最终结果数量
		, NO_10000756_UICODE, INVALID_PLAY_DURATION_RECORD_NUM, NO_PLAY_DURATION_RECORD_NUM, index0_RECORD_NUM, index1_RECORD_NUM, NO_799_OR_004_ACTION, YOUKE_UID_NUM, NO_UID, NO_799_ACTION, TWO_PAGE_RECORD_NUM, ISAUTOPLAY_RECORD_NUM, NO_ISAUTOPLAY_RECORD_NUM, NO_FROM_VAL, IS_1084393010_FROM_VAL, NO_PAGE_Index_RECORD_NUM, NO_RECOM_CODE_RECORD_NUM, VAILD_RECOM_CODE_RECORD_NUM, IS_recom_scene1_RECORD_NUM,NO_INDEX_OR_PAGE_RECORD_NUM,NO_FROM,LESS_3000_RECORD

	}

	// 行为
	public static class videoStreamMapper<K extends LongWritable, V extends BytesRefArrayWritable>
			extends MapReduceBase implements Mapper<K, V, Text,NullWritable> {

		@Override
		public void map(K key, V value, OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			try {
				reporter.incrCounter(Counter.RECORD_NUM, 1);

				// 用户uid
				BytesRefWritable uid_rc = value.get(1);
				String uid = new String(uid_rc.getData(), uid_rc.getStart(), uid_rc.getLength(), "UTF-8");
				if (uid == null || "".equals(uid)) {
					reporter.incrCounter(Counter.NO_UID, 1);
					return;
				}
				// 过滤游客uid
				if (uid.length() > 11) {
					reporter.incrCounter(Counter.YOUKE_UID_NUM, 1);
					return;
				}
				
				// 提取action
				BytesRefWritable action_rc = value.get(2);
				String action = new String(action_rc.getData(), action_rc.getStart(), action_rc.getLength(), "UTF-8");
				// 过滤action!=799字段
				if (!"799".equals(action)) {
					reporter.incrCounter(Counter.NO_799_ACTION, 1);
					return;
				}
				// 提取uicode
				BytesRefWritable uicode_rc = value.get(4);
				String uicode = new String(uicode_rc.getData(), uicode_rc.getStart(), uicode_rc.getLength(), "UTF-8");
				if (uicode == null || "".equals(uicode)) {
					reporter.incrCounter(Counter.NO_UICODE, 1);
					return;
				}
				if (!"10000789".equals(uicode)) {
					reporter.incrCounter(Counter.NO_10000756_UICODE, 1);
					return;
				}
				//提取from 用于区分安卓 ios  安卓page从0开始 ios page从1开始 	
				BytesRefWritable from_rc = value.get(9);
				String from = new String(from_rc.getData(), from_rc.getStart(), from_rc.getLength(), "UTF-8");
				if (from == null || "".equals(from)) {
					reporter.incrCounter(Counter.NO_FROM, 1);
					return;
				}

				// 提取extend字段
				BytesRefWritable extend_rc = value.get(14);
				String extend = new String(extend_rc.getData(), extend_rc.getStart(), extend_rc.getLength(), "UTF-8");
				if (extend == null || "".equals(extend)) {
					reporter.incrCounter(Counter.NO_EXTEND, 1);
					return;
				}
				// 解析extend
				Map<String, String> extendMap = new HashMap<String, String>();
				String[] split = extend.split("\\|");
				int pageNum=0;
				for (String string : split) {
					String[] split2 = string.split(":");
					String mapKey = split2[0];
					String mapValue = "";
					for (int i = 1; i < split2.length; i++) {
						if (i == 1) {
							mapValue = split2[1];
						} else {
							mapValue = mapValue + ":" + split2[i];
						}
					}
					if("page".equals(mapKey)) {
						pageNum++;
					}
					if(pageNum>1) {
						reporter.incrCounter(Counter.TWO_PAGE_RECORD_NUM, 1);
						return;
					}
					extendMap.put(mapKey, mapValue);
				}

				// 获取play_duration
				String valid_play_duration = extendMap.get("valid_play_duration");
				// 过滤没有valid_play_duration的记录
				if (valid_play_duration == null) {
					reporter.incrCounter(Counter.NO_PLAY_DURATION_RECORD_NUM, 1);
					return;
				}
				if(Integer.parseInt(valid_play_duration)<3000){
					reporter.incrCounter(Counter.LESS_3000_RECORD, 1);
					return;	
				}
				// 获取index
				String index = extendMap.get("index");  //index =1 从1开始 到10
				// 获取 page
				String page = extendMap.get("page");  //page=0  从0开始 
				// 无index和page为导流的记录
				if (index == null || page == null) {
					reporter.incrCounter(Counter.NO_INDEX_OR_PAGE_RECORD_NUM, 1);
					return;
				} 			
				String from_tail = from.substring(from.length() - 4, from.length());
				if (from_tail.equals("5010")){ //安卓5010
					return;
				}
				else if(from_tail.equals("3010")){//ios
					//return;
				}

				// 获取uid
				String outKey = uid;
				output.collect(new Text(outKey), NullWritable.get());
				return;

			} catch (Exception e) {
				System.err.println("error in storyBehaviourMapper: " + e);
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
		job.setJobName("TabConsumeUserIos");
		job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
		job.setJarByClass(TabConsumeUserIos.class);

		String[] input_recom_files = otherArgs[0].split(",");
		for (int i = 0; i < input_recom_files.length; i++) {
			MultipleInputs.addInputPath(job, new Path(input_recom_files[i]), RCFileInputFormat.class,
					statistics.tab.TabConsumeUserIos.videoStreamMapper.class);
		}

		//job.setReducerClass(MyReducer.class);

		// 设置MAP输出的key和value的格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

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
