package hdfs.demo;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DirDemo {

	public static void main(String[] args) throws Exception {
		/*
		 * 检查目录路径是否存在，若不存在则创建该目录树
		 */
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://hadoop:9000/output/");
		FileSystem fs = FileSystem.get(uri, conf);
		Boolean dirstatus = fs.mkdirs(new Path(uri));
		System.out.println(dirstatus);

	}

}
