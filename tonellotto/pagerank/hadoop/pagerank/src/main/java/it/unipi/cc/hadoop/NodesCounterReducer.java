package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 */
public class NodesCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    private final IntWritable outputValue = new IntWritable();

    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
	{
		int sum = 0;
        for (final IntWritable val : values) {
            sum += val.get();
        }
        outputValue.set(sum);
        context.write(key, outputValue);
    }
}