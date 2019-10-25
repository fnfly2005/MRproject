package mr.app;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
	/*
	 * MapReduce应用开发-用MRUnit来写单元测试-关于Mapper
	 * 范例6-7 该类解析NCDC格式的气温记录
	 * 用于封装解析逻辑
	 * java String s.charAt()
	 * java 运算符 &&
	 */
	private static final int MISSING_TEMPERATURE = 9999;
	
	private String year;
	private int airTemperature;
	private String quality;
	
	public void parse(String record) {
		year = record.substring(15, 19);
		String airTemperatureString;
		// Remove Leading plus sign as parseInt doesn't like them
		if (record.charAt(87) == '+') {//char charAt(int index) --Returns the char value at the specified index
			airTemperatureString = record.substring(88,92);
		}else {
			airTemperatureString = record.substring(87,92);
		}
		
		airTemperature = Integer.parseInt(airTemperatureString);
		quality = record.substring(92,93);
	}
	
	public void parse(Text record) {
		parse(record.toString());
	}
	
	public boolean isValidTemperature() {
		//boolean matches(String regex) --Tells whether or not this string matches the given regular expression. 
		return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");//&& 逻辑与 当且仅当两个都为真
	}
	
	public String getYear() {
		return year;
	}
	
	public int getAirTemperature() {
		return airTemperature;
	}
}
