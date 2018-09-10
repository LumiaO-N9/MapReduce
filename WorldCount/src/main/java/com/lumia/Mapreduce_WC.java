package com.lumia;

/**
 * Hello world!
 *
 */
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


public class Mapreduce_WC {
	public static class Mymapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		//map方法是作用于每一行数据，参数是三个，key，value，context
		//context 是上下文类，起到承上启下的作用。包含整个任务的信息，以及写入写出的功能 
		@Override
		protected void map(LongWritable key, Text value,				
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//将text类型的整行数据，转成string类型
			 String line = value.toString();
			//根据文本数据的分隔符进行切分为一个数组
			 String[] words = line.split(",");
			//遍历数组，将每个单词写出
			for (String word : words) {
	
				context.write(new Text(word), new LongWritable(1l));
			}
			
		}
		
		
	}
	
	//风骚的分割线------进入reduce阶段---相同的分区通过shuffle会将数据拉取到reduce
	//每一个reduce会接收相同的key的一组数据
	//指定reduce的key ，value的输入输出类型
	public static class Myreducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	//覆盖父类的reduce函数
	//每一个reduce函数处理相同key的一组数据
		@Override
		protected void reduce(Text key2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
		long sum =0l;
		//遍历迭代器中的value，也就是相同key的一组value
		for (LongWritable longvalue : v2s) {
			//将每个value相加
			sum=sum+longvalue.get();
		}
			//将结果数据输出，写出文件到磁盘			
			context.write(key2, new LongWritable(sum));
			
		}
		
		
	}
	
//主函数，程序入口，组装整个mapreduce任务
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//获取默认配置信息，如何想自定义修改一些参数，可以使用 conf.set()传入配置参数
		Configuration conf = new Configuration();
		//创建job任务
		Job job = Job.getInstance(conf, Mapreduce_WC.class.getSimpleName());
		//指定任务类的jar包
		job.setJarByClass(Mapreduce_WC.class);
		//指定读取数据的路径，也就是数据来源在哪里。是指hdfs上的路径,args[0]代表输入的第一个参数
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//指定map类
		job.setMapperClass(Mymapper.class);
		//指定map中间输出结果的key和value的序列化类型
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(LongWritable.class);
		//指定reduce类		
		job.setReducerClass(Myreducer.class);
		//指定输出key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//指定输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//提交任务，等待完成
		job.waitForCompletion(true);
		
		
		
	}
	
	

}
