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
	 * 范例6-5 MaxTemperatureMapper的单元测试
	*/
	@Test
	public void processesValidRecord() throws IOException,InterruptedException{
		
		//Text(String string) 创建一个value字符串
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + 
				"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		
		 
		//初始化一个测试工具类
		//.xxxx 返回self以获得流畅的编程风格(fluent样式)
		new MapDriver<LongWritable,Text,Text,IntWritable>() //MapDriver<K1,V1,K2,V2> 允许您测试映射器实例的工具，该类是MapDriverBase的子类
			.withMapper(new MaxTemperatureMapper1()) //withMapper(Mapper<K1,V1,K2,V2> m) 设置要使用的Mapper实例
			.withInput(new LongWritable(0),value) //withInput(K1 key,V1 val) 继承父类MapDriverBase的方法 等同于setInput() 
			//setInput(K1 key,V1 val) --Sets the input to send to the mapper 
			//LongWritable(long value) --implements WritableComparable<LongWritable> --A WritableComparable for longs 
			.withOutput(new Text("1950"),new IntWritable(-11))//withOutput(K2 key,V2 val) 继承父类MapDriverBase的方法 等同于addOutput()
			//addOutput(K2 key,V2 val) --Adds a (k, v) pair we expect as output
			//IntWritable(int value) ---A WritableComparable for ints
			.runTest();//--Runs the test and validates the results --继承父类MapDriverBase的父类TestDriver的方法
		
	}
	
	//缺失值测试
	@Test
	public void ignoreMissingTemperatureRecord() throws IOException,InterruptedException {
			Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + 
					"99999V0203201N00261220001CN9999999N9+99991+99999999999");
			new MapDriver<LongWritable,Text,Text,IntWritable>()
			.withMapper(new MaxTemperatureMapper())//MaxTemperatureMapper1由于不支持对缺失值的处理，故测试会失败
			.withInput(new LongWritable(0),value)
			.runTest();
		}
	
	/*
	 * MapReduce应用开发-用MRUnit来写单元测试-关于Reducer
	 * 测试reducer 找出指定键最大值的功能
	 */
	@Test
	public void returnsMaximumIntegerInValues() throws IOException,InterruptedException {
		new ReduceDriver<Text,IntWritable,Text,IntWritable>()
			.withReducer(new MaxTemperatureReducer())
			//withReducer(Reducer<K1,V1,K2,V2> r) --等同于setReducer
			//setReducer(Reducer<K1,V1,K2,V2> r) --Sets the reducer object to use for this test  
			.withInput(new Text("1950"),Arrays.asList(new IntWritable(10),new IntWritable(5)))
			//withInput(K1 key, List<V1> values) 继承父类ReduceDriverBase的方法 等同于setInput
			//setInput(K1 key,List<V1> values) 继承父类ReduceDriverBase的方法 Sets the input to send to the reducer
			//static <T> List<T> asList(T... a) 返回由指定数组支持的固定大小列表
			.withOutput(new Text("1950"), new IntWritable(10))
			//继承了父类TestDriver
			//withOutput(K2 key,V2 val) --等同于addOutput
			//addOutput(K2 key,V2 val) --Adds a (k, v) pair we expect as output
			.runTest();
	}
	}

