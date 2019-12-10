package mr.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/*
	 * MapReduce应用开发-用MRUnit来写单元测试-关于Mapper
	 * 范例 6-8 这个mapper 使用utility类来解析记录
	 */
	
	enum Temperature {
		OVER_100//java Enum Enum(String name, int ordinal)
	}
	
	private NcdcRecordParser Parser = new NcdcRecordParser();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		Parser.parse(value);
		if (Parser.isValidTemperature()) {
			int airTemperature =Parser.getAirTemperature();
			if (airTemperature>300) {
				/*
				 * java System err
				 * java PrintStream println()
				*/
				System.err.println("Temperature over 100 degrees for input:" +value);
				//hadoop mapreduce TaskAttemptContext.getStatus()
				context.setStatus("Detected possibly corrupt record: see logs.");
				//hadoop mapreduce TaskAttemptContext.getCounter(String groupName,String counterName)
				context.getCounter(Temperature.OVER_100);
			}
			//hadoop mapreduce TaskInputOutputContext.wirte(KEYOUT key,VALUEOUT value)
			context.write(new Text(Parser.getYear()), new IntWritable(Parser.getAirTemperature()));
		}
	}
	
	

}
