package it.unipi.cc.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class implements the WritableComparator used to sort in descending order
 * the output generated by SorterReducer.
 * 
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class DescendingDoubleWritableComparator extends WritableComparator
{
	protected DescendingDoubleWritableComparator() {
		super(DoubleWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DoubleWritable key1 = (DoubleWritable) w1;
		DoubleWritable key2 = (DoubleWritable) w2;          
		return -1 * key1.compareTo(key2);
	}
}