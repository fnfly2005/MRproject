package hdfs.demo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludePathFilter implements PathFilter {
	/*
	 * PathFilter用于排除匹配正则表达式的路径
	 */
	private final String regex;

	public RegexExcludePathFilter(String regex) {
		// TODO Auto-generated constructor stub
		this.regex = regex;
	}
	
	public boolean accept(Path path) {
		return !path.toString().matches(regex);
	}

}
