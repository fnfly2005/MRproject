package mr.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Meteorological {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length !=2)
		{
			System.err.println("本程序需要两个参数执行，hadoop mr.demo.Meteorological /input/info/txt /output");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		
		String [] argArray = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf,"hadoop");
		job.setJarByClass(Meteorological.class);
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setMapOutputKeyClass(Text.class);//设置输出的key的类型
		job.setMapOutputValueClass(IntWritable.class);//设置输出的value的类型
		
		job.setCombinerClass(MaxTemperatureReducer.class);
		/*
		 *  setReducerClass 适用面广
		 *  setCombinerClass 效率高
		 */
		job.setOutputKeyClass(Text.class); //信息设置为文本
		job.setOutputValueClass(IntWritable.class);//最终内容的设置为一个数值
		
		FileInputFormat.addInputPath(job, new Path(argArray[0]));//设置输入路径
		FileOutputFormat.setOutputPath(job,	new Path(argArray[1]));//设置输出路径		
		
		System.exit(job.waitForCompletion(true) ? 0:1);//执行完毕后退出
	}

}
