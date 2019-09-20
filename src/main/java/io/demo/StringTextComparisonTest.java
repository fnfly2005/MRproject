package io.demo;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class StringTextComparisonTest {
	/*
	 * Hadoop的I/O操作-序列化-Writable类-Text类型二
	 * 范例 5-5 验证String类和Text类的差异性的测试
	 */
	
	@Test
	public void string() throws UnsupportedEncodingException {
		String s = "\u0041\u00DF\u6771\uD801\uDC00";
		assertEquals(s.length(), 5);
		assertEquals(s.getBytes("UTF-8").length, 10);
		
		assertEquals(s.indexOf("\u0041"), 0);
		assertEquals(s.indexOf("\u00DF"), 1);
		assertEquals(s.indexOf("\u6771"), 2);
		assertEquals(s.indexOf("\uD801\uDC00"), 3);
		
		assertEquals(s.charAt(0), '\u0041');
		assertEquals(s.charAt(1), '\u00DF');
		assertEquals(s.charAt(2), '\u6771');
		assertEquals(s.charAt(3), '\uD801');
		assertEquals(s.charAt(4), '\uDC00');
		
		assertEquals(s.codePointAt(0), 0x0041);
		assertEquals(s.codePointAt(1), 0x00DF);
		assertEquals(s.codePointAt(2), 0x6771);
		assertEquals(s.codePointAt(3), 0x10400);
	}

	@Test
	public void text() {
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		
		assertEquals(t.getLength(), 10);
		assertEquals(t.find("\u0041"), 0);
		assertEquals(t.find("\u00DF"), 1);
		assertEquals(t.find("\u6771"), 3);
		assertEquals(t.find("\uD801\uDC00"), 6);
		
		assertEquals(t.charAt(0), 0x0041);
		assertEquals(t.charAt(1), 0x00DF);
		assertEquals(t.charAt(3), 0x6771);
		assertEquals(t.charAt(6), 0x10400);
	}
}
