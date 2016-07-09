package com.mr.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {  
	protected GroupingComparator() {  
		super(TextLong.class, true);  
	}  
	@Override  
    //Compare two WritableComparables.  
    public int compare(WritableComparable w1, WritableComparable w2) {  
		TextLong ip1 = (TextLong) w1;  
        TextLong ip2 = (TextLong) w2;  
        Text l = ip1.getKey();  
        Text r = ip2.getKey();  
        return l.toString().compareTo(r.toString());
	}  
}  
