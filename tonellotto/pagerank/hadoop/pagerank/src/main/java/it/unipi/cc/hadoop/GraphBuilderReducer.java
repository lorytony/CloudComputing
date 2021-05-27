package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class GraphBuilderReducer extends Reducer<Text, Text, Text, Text>
{
	private final Text outputValue = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Configuration conf =  context.getConfiguration();
		int N = Integer.parseInt(conf.get("N"));
		final double initialPageRank = (double)1/N;

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