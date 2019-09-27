package mr.app;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ConfDemo {
	

	@Test
	public void show1() throws IOException {
		/*
		 * MapReduce应用开发-用于配置的API
		 */
		Configuration conf = new Configuration();
		conf.addResource(new File("configuration-1.xml").toURI().toURL());
		
		assertEquals(conf.get("color"), "yellow");
		assertEquals(conf.getInt("size", 0), 10);
		assertEquals(conf.get("breadth","wide"), "wide");
		

		
	}
	
	@Test
	public void show2() throws IOException {
		/*
		 * MapReduce应用开发-用于配置的API-资源合并
		 * MapReduce应用开发-用于配置的API-变量扩展
		 */
		Configuration conf = new Configuration();
		conf.addResource(new File("configuration-1.xml").toURI().toURL());
		conf.addResource(new File("configuration-2.xml").toURI().toURL());//后来添加到资源文件的属性会覆盖之前定义的属性
		
		assertEquals(conf.getInt("size", 0), 12);
		assertEquals(conf.get("weight"),"heavy");//标记为final的属性不能被后面的定义所覆盖，试图覆盖通常意味着配置错误，所以会弹出警告
		
		assertEquals(conf.get("size-weight"), "12,heavy");//配置属性可以用其他属性或系统属性进行定义，例如这些属性使用配置文件中的值来扩展的
		System.setProperty("size", "14");//系统属性的优先级高于资源文件中定义的属性
		assertEquals(conf.get("size-weight"), "14,heavy");
		
		//虽然配置属性可以通过系统属性来定义，但除非系统属性使用配置属性重新定义，否则它们无法通过配置API进行访问的
		System.setProperty("length", "2");
		assertEquals(conf.get("length"), (String) null);
	}
}
