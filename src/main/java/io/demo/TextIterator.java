package io.demo;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

public class TextIterator {
	/*
	 * java.nio包
	 * ByteBuffer 字节缓冲类
	 * wrap() 将字节数组包装到缓冲区
	 * hasRemaining() 检测是否有元素剩余
	 * bytesToCodePoint() 返回缓冲区下一代码的位置
	 * 
	 * Hadoop的I/O操作-序列化-Writable类-Text类型三
	 * 
	 */

	public static void main(String[] args) {
		Text t = new Text("\u0041\u00DF\u6881\uD801\uDC00");
		
		ByteBuffer buf = ByteBuffer.wrap(t.getBytes(),0,t.getLength());
		
		int cp;
		
		while(buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf))!=-1) {
			System.out.println(Integer.toHexString(cp));
		}

	}

}
