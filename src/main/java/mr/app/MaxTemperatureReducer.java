package mr.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	/*
	 * MapReduce应用开发-用MRUnit来写单元测试-关于Reducer
	 * 范例6-9. 用来计算最高气温的reducer
	 * hadoop mapreduce Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 类
	 */

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		//reduce(KEYIN key,Iterable<VALUEIN> values,Reducer.Context context)
		//对每个键调用一次此方法。大多数应用程序将通过重写此方法来定义它们的reduce类
		int maxValue = Integer.MIN_VALUE;//MIN_VALUE A constant holding the minimum value an int can have, -231
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());//Math.max(a,b) Returns the greater of two values
		}//IntWritable.get() Return the value of this IntWritable.
		context.write(key, new IntWritable(maxValue));
	}
	

}
