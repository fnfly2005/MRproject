package io.demo;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;


public class SerializeDemo {

	@Test
	public void writableInterfaceDemo() throws IOException {
		//Hadoop的I/O操作-序列化-Writable接口一
		IntWritable writable = new IntWritable();
		writable.set(163);
		byte[] bytes = serialize(writable);
		
		assertEquals(bytes.length, 4);
		assertEquals(StringUtils.byteToHexString(bytes), "000000a3");
		
		IntWritable newWritable = new IntWritable();
		deserialize(newWritable, bytes);
		assertEquals(newWritable.get(), 163);
		
		//Hadoop的I/O操作-序列化-Writable接口二
		@SuppressWarnings("unchecked")
		RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);

		IntWritable w1 = new IntWritable(163);
		IntWritable w2 = new IntWritable(67);
		// 比较IntWritable对象
		assertEquals(comparator.compare(w1, w2), 1);// 相同为0，1>2为1，1<2为-1

		// 比较序列化对象
		byte[] b1 = serialize(w1);
		byte[] b2 = serialize(w2);
		assertEquals(comparator.compare(b1, 0, b1.length, b2, 0, b2.length), 1);	
	}
	
	@Test
	public void writableClassDemo() throws IOException {
		//Hadoop的I/O操作-序列化-Writable类
		// Java基本类型Writable封装器
		byte[] data = serialize(new VIntWritable(163));
		assertEquals(StringUtils.byteToHexString(data), "8fa3");
		
		//Hadoop的I/O操作-序列化-Writable类-Text类型一
		Text t = new Text("hadoop");
		assertEquals(t.getLength(), 6);
		assertEquals(t.getBytes().length,6);
		assertEquals(t.charAt(2),(int)'d');
		
		assertEquals(t.charAt(100),-1);//Out of bounds
		assertEquals(t.find("do"),2);//find a substring
		assertEquals(t.find("o"),3);//finds first o
		assertEquals(t.find("o",4),4);//finds o from position 4 or later
		assertEquals(t.find("pig"),-1);//No match
	}
	
	public static byte[] serialize(Writable writable) throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();

		return out.toByteArray();
	}

	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;

	}

}
