package hdfs.demo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {

	public static void main(String[] args) throws Exception {
		/*
		 * 将本地文件复制到hdfs
		 */
		String localSrc = "Downloads/blacklistDet_v2.csv";
		String dst = "hdfs://hadoop:9000/output/blacklistDet_v2.csv";
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		URI uri = new URI(dst);
		FileSystem fs = FileSystem.get(uri,conf);
		Progressable pge = new Progressable() {
			
			@Override
			public void progress() {
				System.out.print(".");
			}
		};
 		FSDataOutputStream out = fs.create(new Path(dst),pge);
 		
 		IOUtils.copyBytes(in, out, 4096,true);

	}

}
