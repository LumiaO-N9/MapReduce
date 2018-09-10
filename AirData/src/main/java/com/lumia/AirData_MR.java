package com.lumia;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AirData_MR {
	public static class AirMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//将行数据转成字符串
			String line = value.toString();
			//切割整行数据
			String[] filed = line.split(",");
			//过滤出PM2.5的数据
			if (filed[2].equals("PM2.5")) {
				//|filed[1].equals("4") 如果想过滤出某个时间点的数据
				for (int i = 3,j=1; i < filed.length; i++,j++) {
					if ("".equals(filed[i])||filed[i].equals(null)||filed[i].equals(" ")) {
						filed[i]="0";
					}
					context.write(new Text(filed[0]+","+j),new Text(filed[i]));

				}												
			}		
			
		}
	}
	
	public static class AirReducer extends Reducer<Text, Text, Text, LongWritable>{

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long sum=0l;
			for (Text text : v2s) {
				long pm = Long.parseLong(text.toString());
				sum=sum+pm;
				}	
			long avg=sum/24;
			context.write(k2, new LongWritable(avg));
		}

	}
		
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		//设置输出文件key和value分隔符
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf,AirData_MR.class.getSimpleName());
		//必须指定
		job.setJarByClass(AirData_MR.class);		
		//数据来自哪里
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		//自定义的mapper在哪里
		job.setMapperClass(AirMapper.class);
		//指定mapper输出的<k2,v2>的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//自定义的reducer在哪里
		job.setReducerClass(AirReducer.class);
		//指定reducer输出的<k3,v3>的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//job.setNumReduceTasks(0); 
		//数据写到哪里
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
