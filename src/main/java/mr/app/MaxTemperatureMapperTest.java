package mr.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import mr.app.MaxTemperatureMapper;

public class MaxTemperatureMapperTest {
	/*
	 * MapReduce应用开发-用MRUnit来写单元测试-关于Mapper
	 
	
	@Test
	public void processesValidRecord() throws IOException,InterruptedException{
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + 
				"99999V0203201N00261220001CN9999999N9+99991+99999999999");
		new MapDriver<LongWritable,Text,Text,IntWritable>()
			.withMapper(new MaxTemperatureMapper())
			.withInput(new LongWritable(0),value)
			.runTest();
	}*/
	
	@Test
	public void ignoreMissingTemperatureRecord() throws IOException,InterruptedException {
			Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + 
					"99999V0203201N00261220001CN9999999N9+99991+99999999999");
			new MapDriver<LongWritable,Text,Text,IntWritable>()
			.withMapper(new MaxTemperatureMapper())
			.withInput(new LongWritable(0),value)
			.runTest();
		}
	}

