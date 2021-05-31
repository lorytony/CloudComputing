package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text>
{
	private final Text outputValue = new Text();
	private double alfa;
	private int N;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		final Configuration conf =  context.getConfiguration();
		alfa = Double.parseDouble(conf.get("ALFA"));
		N = Integer.parseInt(conf.get("N"));
	}

	@Override
	public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		double sum = 0.0;
		String links = "";

		for (final Text value : values) {
			final String valueString = value.toString();

			if (valueString.startsWith("-structure-")) {
				links = valueString.substring(11);
			} else {
				double p = Double.parseDouble(valueString);
				sum += p;
			}
		}

		double pr = alfa*((double)1/N) + (1 - alfa)*sum;
		outputValue.set(pr + "\t" + links);
		context.write(key, outputValue);
	}
}