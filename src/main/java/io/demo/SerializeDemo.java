package io.demo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;


public class SerializeDemo {

	public static void main(String[] args) throws IOException {
		/*
		 * Hadoop的I/O操作-序列化-Writable接口一
		 */
		IntWritable writable1 = new IntWritable();
		writable1.set(163);
		IntWritable writable2 = new IntWritable(162);
		byte[] bytes = serialize(writable1);
		
		
		System.out.println(writable1 + ":" + writable2 + ":" + bytes.length);
		System.out.println(StringUtils.byteToHexString(bytes));
		
		IntWritable newWritable = new IntWritable();
		deserialize(newWritable, bytes);
		System.out.println(newWritable.get());

	}
	
	public static byte[] serialize(Writable writable) throws IOException{
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		
		return out.toByteArray();
	}
	
	public static byte[] deserialize(Writable writable,byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
		
	}

}
