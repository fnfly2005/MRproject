package io.demo;

import java.io.*;
import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

	/*
	 * Hadoop的I/O操作-序列化-实现定制的Writable集合
	 */
	private Text first;
	private Text second;
	
	public TextPair() {
		// TODO Auto-generated constructor stub
		set(new Text(), new Text());
	}
	
	public TextPair(String first,String second) {
		set(new Text(first), new Text(second));
	}
	
	public TextPair(Text first, Text second) {
		set(first, second);
	}
	
	public void set(Text first, Text second) {
		this.first=first;
		this.second=second;
	}
	
	public Text getFirst() {
		return first;
	}
	
	public Text getSecond() {
		return second;
	}

	
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	
	public int compareTo(TextPair tp) {
		int cmp = first.compareTo(tp.first);
		
		if(cmp != 0) {
			return cmp;
		}
		
		return second.compareTo(tp.second);
	}

	@Override
	public int hashCode() {
		return first.hashCode()*163 + second.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof TextPair) {
			TextPair tp = (TextPair) obj;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	public static class Comparator extends WritableComparator{
		/*
		 * Hadoop的I/O操作-序列化-实现定制的Writable集合-为提高速度实现一个RawComparator
		 */
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		public Comparator() {
			super(TextPair.class);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1,
						   byte[] b2, int s2, int l2) {
			try {
				
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])+ readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1,firstL1,b2,s2,firstL2);
				if(cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1+firstL1, l1-firstL1, 
											   b2, s2+firstL2, l2-firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
			
		}
		
		static {
			WritableComparator.define(TextPair.class, new Comparator());
		}
	}
	
	public static class FirstComparator extends WritableComparator {
		/*
		 * Hadoop的I/O操作-序列化-实现定制的Writable集合-定制的comparator
		 */
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		public FirstComparator() {
			// TODO Auto-generated constructor stub
			super(TextPair.class);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 =WritableUtils.decodeVIntSize(b1[s1])+ readVInt(b1, s1);
				int firstL2 =WritableUtils.decodeVIntSize(b2[s2])+ readVInt(b2, s2);
				return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				// TODO: handle exception
				throw new IllegalArgumentException(e);				
			}
			
			
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof TextPair && b instanceof TextPair) {
				return ((TextPair) a).first.compareTo(((TextPair) b).first);
			}
			return super.compare(a, b);
		}
		
		
	}
	
}


