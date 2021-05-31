package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class GraphBuilderReducer extends Reducer<Text, Text, Text, Text>
{
	private final Text outputValue = new Text();
	private double initialPageRank;
	private int N;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		final Configuration conf = context.getConfiguration();
		N = Integer.parseInt(conf.get("N"));
		initialPageRank = (double)1/N;
	}

	@Override
	public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		boolean firstLink = true;
		String output = initialPageRank + "\t";
		for (final Text value : values) {
			if (!firstLink) output += "]]";
			output += value.toString();
			firstLink = false;
		}
		outputValue.set(output);
		context.write(key, outputValue);
	}
}