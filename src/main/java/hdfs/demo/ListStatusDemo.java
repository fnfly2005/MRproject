package hdfs.demo;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatusDemo {

	public static void main(String[] args) throws Exception{
		/*
		 * 分布式文件系统-JAVA接口-查询文件系统
		 */
		String uri = "hdfs://hadoop:9000/";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(uri), conf);
		Path[] paths = new Path[args.length];
		for (int i =0;i<paths.length;i++) {
			paths[i] = new Path(args[i]);
		}
		FileStatus[] status = fs.listStatus(paths);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p:listedPaths) {
			System.out.println(p);
		}
		

	}

}
