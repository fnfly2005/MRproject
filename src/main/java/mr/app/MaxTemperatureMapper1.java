package mr.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
/*
 * MapReduce应用开发-用MRUnit来写单元测试-关于Mapper
 * 范例6-6 通过了MaxTemperatureMapper测试的第一个版本Mapper函数
 * hadoop Mapper MapContext
 * hadoop text t.toString()
 * java Integer parseInt() 
 * hadoop TaskInputOutputContext c.write()
 */
	@Override
	public void map(LongWritable key, Text value, Context context)//MapContext --The context that is given to the Mapper
			throws IOException, InterruptedException {
		
		String line = value.toString();//String t.toString() --Convert text back to string
		String year = line.substring(15,19);
		int airTemperature = Integer.parseInt(line.substring(87,92));//int parseInt(String s) --Parses the string argument as a signed decimal integer
		context.write(new Text(year), new IntWritable(airTemperature));//void c.wirte(KEYOUT key,VALUEOUT value) --Generate an output key/value pair
		
	}
	
	

}
