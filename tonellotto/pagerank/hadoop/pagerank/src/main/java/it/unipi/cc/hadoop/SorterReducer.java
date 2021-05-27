package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class SorterReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
{
    public void reduce(final DoubleWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		for (final Text title : values) {
			context.write(title, key);
		}
    }
}