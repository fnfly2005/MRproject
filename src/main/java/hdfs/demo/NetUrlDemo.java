package hdfs.demo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class NetUrlDemo {

	public static void main(String[] args) throws Exception{
		/*
		 * 从HadoopURL读取数据
		 */
        
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());//注册HDFS流处理器
		
		InputStream in = null;
		try {
			//hdfs hadoop1.x原有端口为8020,hadoop2.x改为9000
			in = new URL("hdfs://hadoop:9000/input/mdln.txt").openStream();
			
			//IO类 copyBytes方法参数：输入流、输出流、缓冲区大小、复制结束是否关闭数据流
			IOUtils.copyBytes(in, System.out, 4096,false);//读取流文件
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			IOUtils.closeStream(in);
		}

	}

}
