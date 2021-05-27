package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text>
{
	private final Text outputValue = new Text();

	@Override
	public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		Configuration conf =  context.getConfiguration();
		final int N = Integer.parseInt(conf.get("N"));
		final float alfa = Float.parseFloat(conf.get("ALFA"));

		double sum = alfa*((double)1/N);
		String links = "";

		for (final Text value : values) {
			final String valueString = value.toString();

			if (valueString.startsWith("-structure-")) {
				links = valueString.substring(11);
			} else {
				double p = Double.valueOf(valueString);
				sum += p;
			}
		}

		double pr = (1-alfa)*sum;
		outputValue.set(pr + "\t" + links);
		context.write(key, outputValue);
	}
}