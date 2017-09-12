package WeChat;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

public class WeChatMapper {

	/**
	 * @param args
	 */
	enum WeChatMappper{COUNT_NULLNUM,COUNT_NUM,COUNT_SUM}
	public static class WCMapper extends Mapper<LongWritable,Text,WeChatw,NullWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String josn=value.toString().substring(1,value.toString().length()-1);
		    ObjectMapper om=new ObjectMapper();
		    WeChatw wei=om.readValue(josn, WeChatw.class);
		    if(wei.getUserid()==""||wei.getChatid()==null){
		    	context.getCounter(WeChatMappper.COUNT_NULLNUM).increment(1);
		    }
		    else if (wei.getUserid()==""||wei.getUserid()==null) {
				context.getCounter(WeChatMappper.COUNT_NUM).increment(1);
			}
		    else {
				context.getCounter(WeChatMappper.COUNT_SUM).increment(1);
			}
		    context.write(wei,NullWritable.get());
		    
		}
		
		
		
	}
	public static class WCReducer extends Reducer<WeChatw,NullWritable,WeChatw,NullWritable>{

		@Override
		protected void reduce(WeChatw we, Iterable<NullWritable> u,
				Context ce)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ce.write(we, NullWritable.get());
		}
		    
		
		
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=new Job(conf, "Wechatjob");
		job.setJarByClass(WeChatMapper.class);
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(WeChatw.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path("file:///user/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs:///user/input"));
        System.exit(job.waitForCompletion(true)?1:0);
	}

}
