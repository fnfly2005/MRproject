package io.demo;

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

public class SerializeDemo {

	public static void main(String[] args) throws IOException {
		System.out.println("Hadoop的I/O操作-序列化-Writable接口一");
		IntWritable writable1 = new IntWritable();
		writable1.set(163);
		IntWritable writable2 = new IntWritable(164);
		byte[] bytes = serialize(writable1);

		System.out.println(writable1 + ":" + writable2 + ":" + bytes.length);
		System.out.println("序列化："+StringUtils.byteToHexString(bytes));

		IntWritable newWritable = new IntWritable();
		deserialize(newWritable, bytes);
		System.out.println("反序列化："+newWritable.get());

		System.out.println("Hadoop的I/O操作-序列化-Writable接口二");
		@SuppressWarnings("unchecked")
		RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);

		// 比较IntWritable对象
		System.out.println("常规数据比较："+comparator.compare(writable1, writable2));// 相同为0，1>2为1，1<2为-1

		// 比较序列化对象
		byte[] b1 = serialize(writable1);
		byte[] b2 = serialize(writable2);
		System.out.println("原始字节数据比较："+comparator.compare(b1, 0, b1.length, b2, 0, b2.length));

		System.out.println("Hadoop的I/O操作-序列化-Writable类");
		// Java基本类型Writable封装器
		byte[] data = serialize(new VIntWritable(163));
		System.out.println(StringUtils.byteToHexString(data));
		
		System.out.println("Hadoop的I/O操作-序列化-Writable类-Text类型");
		Text t = new Text("hadoop");
		System.out.println(t.getLength());
		System.out.println(t.getBytes().length);
		System.out.println(t.charAt(2));
		System.out.println(t.charAt(100));
		System.out.println("find a substring:" + t.find("do"));
		System.out.println("finds first o:" + t.find("o"));
		System.out.println("finds o from position 4 or later:" + t.find("o",4));
		System.out.println("No match:"+t.find("pig"));

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
