package statistics.shortvideo;

//导入时间类
//用户协同  item协同的数据制备
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;
import java.util.Iterator;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.avro.mapred.SequenceFileInputFormat;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.math3.analysis.function.Log10;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.mapred.Reducer;

import com.hadoop.mapred.DeprecatedLzoTextInputFormat;

import antlr.StringUtils;
import io.netty.util.internal.StringUtil;

import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;

/*
*提取导流横版视频，提取竖版视频
*计算导流mid的roi = 导流微博的有效播放次数 / 导流微博的导流次数
* 
*/
public class DaoLiuRecordGet {
	private static enum Counter {
		RECORD_NUM, NO_EXTEND, INVALID_RECORD_NUM, RESULT_NUM // 最终结果数量
		, NO_799_ACTION, NO_10000376_UICODE, NO_VALID_PLAY_DURATION_RECORD_NUM, NO_LMID_RECORD_NUM, INVALID_VALID_PLAY_DURATION_RECORD_NUM, NO_INDEX_RECORD_NUM, NO_MID_RECORD_NUM, NO_MID_NUM, NO_previousId_UICODE, NO_previous_uicode_UICODE, index0_RECORD_NUM, index1_RECORD_NUM, NO_AUTHORID_RECORD_NUM, NO_TIME_RECORD_NUM, NO_UID_RECORD_NUM,RECONM_SCENE
	}

	// 通过mid对源日志文件处理
	public static class daoLiuUserMapper<K extends LongWritable, V extends BytesRefArrayWritable> extends MapReduceBase
			implements Mapper<K, V, Text, Text> {

		
		public static String getExceptionAllinformation(Exception ex) {
			String sOut = "";
			StackTraceElement[] trace = ex.getStackTrace();
			for (StackTraceElement s : trace) {
				sOut += "\tat " + s + "\r\n";
			}
			return sOut;
		}

		@Override
		public void map(K key, V value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {
				reporter.incrCounter(Counter.RECORD_NUM, 1);
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
				// 过滤uicode!="10000376"字段
				if (!"10000376".equals(uicode) && !"10000756".equals(uicode)) {
					reporter.incrCounter(Counter.NO_10000376_UICODE, 1);
					return;
				}
				
				// 提取extend字段
				BytesRefWritable extend_rc = value.get(14);
				String extend = new String(extend_rc.getData(), extend_rc.getStart(), extend_rc.getLength(), "UTF-8");
				if (extend == null || "".equals(extend)) {
					reporter.incrCounter(Counter.NO_EXTEND, 1);
					return;
				}
				Map<String, String> extendMap = new HashMap<String, String>();
				String[] split = extend.split("\\|");
				for (String string : split) {
					String[] split2 = string.split(":");
					if (split2.length == 2) {
						extendMap.put(split2[0], split2[1]);
					} else if (split2.length == 1) {
						extendMap.put(split2[0], "");
					}
				}
				// 获取valid_play_duration
				String valid_play_duration = extendMap.get("valid_play_duration");

				// 过滤没有valid_play_duration或valid_play_duration <= 3000的记录
				if (valid_play_duration != null) {
					if(!"".equals(valid_play_duration) && isInteger(valid_play_duration) ){
						if (Integer.valueOf(valid_play_duration) <= 3000) {
							reporter.incrCounter(Counter.INVALID_VALID_PLAY_DURATION_RECORD_NUM, 1);
							return;
						}
					}else {
						reporter.incrCounter(Counter.INVALID_VALID_PLAY_DURATION_RECORD_NUM, 1);
						return;
					}
				} else {
					reporter.incrCounter(Counter.NO_VALID_PLAY_DURATION_RECORD_NUM, 1);
					return;
				}
				
				// 获取recom_scene
				String recom_scene = extendMap.get("recom_scene");

				// 过滤没有recom_scene
				if ((recom_scene!=null) &&(recom_scene.equals("1"))) {
					reporter.incrCounter(Counter.RECONM_SCENE, 1);
					return;
				}
				
				// 获取index
				String index = extendMap.get("index");
				// 判断播放类型
				String play_type = "other";// 0导流，1推荐微博
				// 过滤无index的记录
				if (index != null) {
					if ( "null".equals(index)||"NULL".equals(index) || "0".equals(index) || "".equals(index)) {
						reporter.incrCounter(Counter.index0_RECORD_NUM, 1);
						play_type = "0";
					} else if (Long.valueOf(index) >= 1) {
						play_type = "1";
						reporter.incrCounter(Counter.index1_RECORD_NUM, 1);
						return;
					}
				} else {
						reporter.incrCounter(Counter.index0_RECORD_NUM, 1);
						play_type = "0";
				}
				
				// 获取mid
				String mid = extendMap.get("mid");
				if(mid == null || "".equals(mid) ) {
					reporter.incrCounter(Counter.NO_MID_NUM, 1);
					return;
				}
				
				// 提取uid字段
				BytesRefWritable uid_rc = value.get(1);
				String uid = new String(uid_rc.getData(), uid_rc.getStart(), uid_rc.getLength(), "UTF-8");
				if(uid ==null || "".equals(uid)) {
					reporter.incrCounter(Counter.NO_UID_RECORD_NUM, 1);
					return;
				}
				if(uid.length()>11)return;
				
				String valueInfo = mid; 
				output.collect(new Text(uid), new Text(valueInfo));
				reporter.incrCounter(Counter.RESULT_NUM, 1);
				return;
			} catch (Exception e) {
				// System.err.println("error in storyBehaviourMapper: " + e);
				System.err.println(getExceptionAllinformation(e));
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
		job.setJobName("DaoLiuRecordGet");
		job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
		job.setJarByClass(DaoLiuRecordGet.class);

		String[] input_recom_files = otherArgs[0].split(",");
		for (int i = 0; i < input_recom_files.length; i++) {
			MultipleInputs.addInputPath(job, new Path(input_recom_files[i]), RCFileInputFormat.class,
					statistics.shortvideo.DaoLiuRecordGet.daoLiuUserMapper.class);
		}


		// 设置MAP输出的key和value的格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

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
	  * @param str 传入的字符串 
	  * @return 是整数返回true,否则返回false 
	  */
	  public static boolean isInteger(String str) {  
	        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");  
	        return pattern.matcher(str).matches();  
	  }
}
