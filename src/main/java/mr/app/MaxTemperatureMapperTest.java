package mr.app;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import mr.app.MaxTemperatureMapper;
import mr.app.MaxTemperatureReducer;

public class MaxTemperatureMapperTest {
	/*
	 * MapReduce应用开发-用MRUnit来写单元测试-关于Mapper
	 
	*/
	@Test
	public void processesValidRecord() throws IOException,InterruptedException{
		
		//Text(String string) 创建一个value字符串
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + 
				"99999V0203201N00261220001CN9999999N9+99991+99999999999");
		
		 
		//初始化一个测试工具类
		//.xxxx 返回self以获得流畅的编程风格(fluent样式)
		new MapDriver<LongWritable,Text,Text,IntWritable>() //MapDriver<K1,V1,K2,V2> 允许您测试映射器实例的工具，该类是MapDriverBase的子类
			.withMapper(new MaxTemperatureMapper()) //withMapper(Mapper<K1,V1,K2,V2> m) 设置要使用的Mapper实例
			.withInput(new LongWritable(0),value) //withInput(K1 key,V1 val) 继承父类MapDriverBase的方法 等同于setInput() 
			//setInput(K1 key,V1 val) --Sets the input to send to the mapper 
			//LongWritable(long value) --implements WritableComparable<LongWritable> --A WritableComparable for longs 
			.runTest();//--Runs the test and validates the results --继承父类MapDriverBase的父类TestDriver的方法
	}
	
	
	@Test
	public void ignoreMissingTemperatureRecord() throws IOException,InterruptedException {
			Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + 
					"99999V0203201N00261220001CN9999999N9+99991+99999999999");
			new MapDriver<LongWritable,Text,Text,IntWritable>()
			.withMapper(new MaxTemperatureMapper())
			.withInput(new LongWritable(0),value)
			.runTest();
		}
		
	@Test
	public void returnsMaximumIntegerInValues() throws IOException,InterruptedException {
		new ReduceDriver<Text,IntWritable,Text,IntWritable>()
			.withReducer(new MaxTemperatureReducer())
			.withInput(new Text("1950"),Arrays.asList(new IntWritable(10),new IntWritable(5)))
			.withOutput(new Text("1950"), new IntWritable(10))
			.runTest();
	}
	}

