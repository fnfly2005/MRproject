package io.demo;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class FileDecompressor {

	public static void main(String[] args) throws Exception {
		/*
		 * Hadoop的I/O操作-压缩-codec二
		 * 通过CompressionCodecFactory推断CompressionCodec
		 * 该应用根据文件扩展名选取codec解压缩文件
		 */
		String uri = "hdfs://hadoop:9000/test/bltest.gz";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(uri), conf);
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if(codec == null) {
			System.err.println("No codec found for "+uri);
			System.exit(1);
		}
		String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		
		InputStream in = null;
		OutputStream out = null;
		
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
		
		
 
	}

}
