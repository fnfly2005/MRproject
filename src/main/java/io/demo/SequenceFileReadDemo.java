package io.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadDemo {

	public static void main(String[] args) throws IOException {
		/*
		 * Hadoop的I/O操作-基于文件的数据结构-SequenceFile-读操作
		 * 该程序用于处理有Writable类型键、值对的任意一个顺序文件
		 */
		String uri = "numbers.seq";
		Configuration conf = new Configuration();
		
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		
		try {
			SequenceFile.Reader.Option optionfile = Reader.file(path);
			
			reader = new SequenceFile.Reader(conf, optionfile);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while(reader.next(key,value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";//给调用同步标记的行用*号标出
				System.out.printf("[%s%s]\t%s\t%s\n", position,syncSeen,key,value);
				position = reader.getPosition();//beginning of next record
			}		
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(reader);
		}

	}

}
