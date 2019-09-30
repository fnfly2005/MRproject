package mr.app;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool {
/*
 * MapReduce应用开发-配置开发环境-辅助类GenericOptionsParser,Tool和ToolRunner
 * Tool实现用于打印一个Configuration对象属性的范例
 */
	
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("yarn-default.xml");
		Configuration.addDefaultResource("yarn-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf =getConf();
		for (Entry<String,String> entry: conf) {
			System.out.printf("%s=%s\n", entry.getKey(),entry.getValue());
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);

	}

}
