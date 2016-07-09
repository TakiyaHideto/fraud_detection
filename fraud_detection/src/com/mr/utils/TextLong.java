package com.mr.utils;

import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
  
/** 
 * A {@link KeyValueWritable} of {@link Text} keys and  
 * {@link LongWritable} values.  
 */  
public class TextLong extends KeyValueWritable<Text, LongWritable> {  
  
	public TextLong() {  
		key = new Text();  
		value = new LongWritable();  
	}  
    
} 
