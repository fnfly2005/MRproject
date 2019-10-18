package mr.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private NcdcRecordParser Parser = new NcdcRecordParser();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		Parser.parse(value);
		if (Parser.isValidTemperature()) {
			context.write(new Text(Parser.getYear()), new IntWritable(Parser.getAirTemperature()));
		}
	}
	
	

}
