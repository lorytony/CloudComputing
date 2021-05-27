package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This reducer is used by the job in charge of counting the number of nodes of the
 * hyperlink graph. In the reduce() method, the 1 values outputted by the mapper are
 * simply summed to obtain the final count of the nodes in the hyperlink graph.
 *
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
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