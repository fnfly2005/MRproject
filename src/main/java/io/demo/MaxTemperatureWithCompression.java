package io.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mr.demo.MaxTemperatureMapper;
import mr.demo.MaxTemperatureReducer;
import mr.demo.Meteorological;

public class MaxTemperatureWithCompression {

	public static void main(String[] args) throws Exception {
		/*
		 * Hadoop的I/O操作-压缩-在MapReduce中使用压缩
		 * 对查找最高气温作业所产生输出进行压缩
		 */
		
		if (args.length !=2)
		{
			System.err.println("本程序需要两个参数执行，hadoop io.demo.MaxTemperatureWithCompression /input/1901 /output");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"hadoop");//创建无特定集群，及给定作业名称的新作业，需要时会从conf参数创建集群
		job.setJarByClass(Meteorological.class);//帮助hadoop找出它应该向节点发送哪个jar来执行map和reduce任务
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(MaxTemperatureMapper.class);
		
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
