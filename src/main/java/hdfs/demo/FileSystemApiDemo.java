package hdfs.demo;

import java.io.DataOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemApiDemo {

	public static void main(String[] args) throws Exception {
		/*
		 * 分布式文件系统-JAVA接口-FileSystemAPI
		 * 无需调用静态方法注册流处理器，意味着多个组件可同时调用
		 * 使用seek方法，将HADOOP文件系统中的一个文件在标准输出上显示两次
		 */
		String uri = "hdfs://hadoop:9000/input/mdln.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);//URI.CREATE(STRING) 等同 NEW URI(STRING)
		FSDataInputStream in = null;
		DataOutputStream dos = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096,false);
			in.seek(0);//定位到文件初始位置，支持随机访问-多线程
			
			//使用read方法读取文件，同IO流类似
			dos = new DataOutputStream(System.out);
			byte[] buff = new byte[1024];
			int line = 0;
			while((line=in.read(buff))!=-1) {
				dos.write(buff,0,line);
			}
		} 
		finally {
			dos.close();
			IOUtils.closeStream(in);
		}

	}

}
