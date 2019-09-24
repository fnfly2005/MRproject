package io.demo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteDemo {

	/*
	 * Hadoop的I/O操作-基于文件的数据结构-SequenceFile-写操作
	 */
	private static final String[] DATA = {
		"One,two,buckle my shoe",
		"Three, four, shut the door",
		"Five, six, pick up sticks",
		"Seven, eight, lay them straight",
		"Nine, ten, a big fat hen"
	};
	
	public static void main(String[] args) throws IOException {
		String uri = "numbers.seq";
		Configuration conf = new Configuration();
		//FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		
		try {
			SequenceFile.Writer.Option optionfile = Writer.file(path);
			SequenceFile.Writer.Option optionkey = Writer.keyClass(key.getClass());
			SequenceFile.Writer.Option optionvalue = Writer.valueClass(value.getClass());		
			writer = SequenceFile.createWriter(conf,optionfile,optionkey,optionvalue);
			
			for (int i=0; i<100; i++) {
				key.set(100-i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(),key,value);
				writer.append(key, value);
			}
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(writer);
		}
		
	}

}
