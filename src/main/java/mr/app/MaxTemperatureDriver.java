package mr.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureDriver extends Configured implements Tool {
	//Configured 用于可以使用Configuration 的基类 
	//Tool 处理命令行的工具包
	/*
	 * MapReduce应用开发-本地运行测试数据
	 * 范例6-10 查找最高气温
	 */

	@Override
	public int run(String[] args) throws Exception {
		//Tool.run(String[] args) Execute the command with the given arguments.
		if(args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			//getClass() Returns the runtime class of this Object
			//getSimpleName() Returns the simple name of the underlying class as given in the source code
			ToolRunner.printGenericCommandUsage(System.err);
			//ToolRunner can be used to run classes implementing Tool interface
			return -1;
		}
		
		Configuration conf = new Configuration();//Configuration() Provides access to configuration parameters
		Job job = Job.getInstance(conf,"Max temperature");//static Job getInstance(Configuration conf,String jobName) 创建作业并返回一个job实例 
		job.setJarByClass(getClass());//通过查找给定类的来源来设置jar --Set the Jar by finding where a given class came from
		
		FileInputFormat.addInputPath(job, new Path(args[0]));//void addInputPath(Job job,Path path) --Add a Path to the list of inputs for the map-reduce job.
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//void setOutputPath(Job job,Path outputDir) --Set the Path of the output directory for the map-reduce job
		
		job.setMapperClass(MaxTemperatureMapper.class);//void setMapperClass(Class<? extends Mapper> cls) --Set the Mapper for the job
		job.setCombinerClass(MaxTemperatureReducer.class);//void setCombinerClass(Class<? extends Reducer> cls) --Set the combiner class for the job
		job.setReducerClass(MaxTemperatureReducer.class);//setReducerClass(Class<? extends Reducer> cls) --Set the Reducer for the job
		
		job.setOutputKeyClass(Text.class);//void setOutputKeyClass(Class<?> theClass) --Set the key class for the job output data
		job.setOutputValueClass(IntWritable.class);//setOutputValueClass(Class<?> theClass) --Set the value class for job outputs
		
		return job.waitForCompletion(true) ? 0:1;//boolean waitForCompletion(boolean verbose) --Submit the job to the cluster and wait for it to finish
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(),args);//int run(Tool tool,String[] args) --Runs the Tool with its Configuration
		System.exit(exitCode);
	}

}
