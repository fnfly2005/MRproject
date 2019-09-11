package io.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

public class PooledStreamCompressor {

	public static void main(String[] args) throws Exception {
		/*
		 * Hadoop的I/O操作-压缩-codec三
		 * CodecPool
		 * 使用压缩池对读取自标准输入的数据进行压缩，然后将其写到标准输出
		 */
		String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
		
		Class<?> codecClass = Class.forName(codecClassname);
		
		Configuration conf = new Configuration();
		
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
		
		Compressor compressor = null;
		
		try {
			compressor = CodecPool.getCompressor(codec);//从新的CompressionCodec 中获取压缩器
			CompressionOutputStream out = codec.createOutputStream(System.out,compressor);//使用指定压缩器创建数据流
			IOUtils.copyBytes(System.in, out, 4096,false);
			out.finish();
		} finally {
			// 将压缩器返回池中
			CodecPool.returnCompressor(compressor);
		}

	}

}
