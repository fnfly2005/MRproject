package mr.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {//本处要求实现单词统计的处理操作
	//在整个代码中最为关键的部分就是MAP部分与Reduce部分，需要用户自己实现
	/**
	 * 本操作主要是进行map的数据处理
	 * @author fnfly
	 * 在Mapper父类库要接收的内容如下：
	 * Object: 输入数据的具体内容
	 * Text：每行的文本数据
	 * Text:  每个单词分解后的统计结果
	 * IntWritable： 输出记录的结果
	 */
	
	private static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>
	{

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 默认情况下是取得每行的数据，所以每行的数据里面都会存在有空格，那么要按照空格进行拆分，每当出现一个单词就需要做一个统计的1
			String lineContent = value.toString(); //取出每行的数据
			String result [] = lineContent.split(" "); //按照空格进行数据拆分
			for (int x =0 ; x<result.length; x++) //循环每个单词而后进行数据的生成
			{// 每一个单词最终生成的保存个数是1
				context.write(new Text(result[x]),new IntWritable(1));
			}
		}
		/**
		 * 进行合并后数据的最终统计
		 * @author fnfly
		 * 本次要使用的类型信息如下
		 * Text:Map输出的文本内容
		 * IntWritable:Map处理的个数
		 * Text: Reduce 输出文本
		 * IntWritable:Reduce的输出个数
		 */
		
	}
		private static class WordCountReduce extends Reducer<Text, IntWritable,Text, IntWritable>
		{

			@Override
			protected void reduce(Text key, Iterable<IntWritable> values,
					Reducer<Text, IntWritable, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
				int sum = 0; //保存每个单词出现的数据量
				for (IntWritable count: values) {
					sum += count.get();
				}
				context.write(key, new IntWritable(sum));
			}
			
		}
		public static void main(String[] args) throws Exception {
			if (args.length !=2)
			{
				System.out.println("本程序需要两个参数执行，hadoop jar wordcount.jar /input/info/txt /output");
				System.exit(1);
			}
			// 每一次的执行实际上都属于一个作业（Job) ，但是现在希望可以通过初始化参数来设置HDFS的文件存储的路径
			// 假设现在的文件保存在HDFS的"/input/info.txt"上，而且最终的输出结果也将保存在HDFS的"/output"目录中
			Configuration conf = new Configuration();//进行相关配置使用
			//考虑到最终需要使用HDFS进行内容的处理操作，并且输入的时候不带有HDFS地址
			String [] argArray = new GenericOptionsParser(conf, args).getRemainingArgs();//对输入的参数进行处理
			// 后面就需要通过作业进行处理了，而且Map 与Reduce 操作必须通过作业来配置
			Job job = Job.getInstance(conf,"hadoop");//定义一个Hadoop的作业
			job.setJarByClass(WordCount.class);//设置执行的jar文件的程序类
			job.setMapperClass(WordCountMapper.class);//指定Mapper的处理类
			job.setMapOutputKeyClass(Text.class);//设置输出的key的类型
			job.setMapOutputValueClass(IntWritable.class);//设置输出的value的类型
			job.setReducerClass(WordCountReduce.class);//设置reduce操作的处理类
			//随后需要设置Map-Reduce最终的执行结果
			job.setOutputKeyClass(Text.class); //信息设置为文本
			job.setOutputValueClass(IntWritable.class);//最终内容的设置为一个数值
			//设置输入以及输出路径
			FileInputFormat.addInputPath(job, new  Path(argArray[0]));//设置输入路径
			FileOutputFormat.setOutputPath(job,	new Path(argArray[1]));//设置输出路径
			// 等待执行完毕
			System.exit(job.waitForCompletion(true) ? 0:1);//执行完毕后退出
			
		}
}
