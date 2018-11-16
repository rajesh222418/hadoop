package org.hadoop.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortCompKeySortComparator extends WritableComparator {

  protected SecondarySortCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getFname().compareTo(key2.getFname());
		if (cmpResult == 0)// same zip
		{
			
				cmpResult = -key1.getLname().compareTo(key2.getLname());
			
		}
		return cmpResult;
	}
}
