package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private final Text outputKey = new Text();
	private final Text outputValue = new Text();

	@Override
	public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		final String line = value.toString();
		final String tokens[] = line.split("\\t");
		final String title = tokens[0];
		final String initialPageRank = tokens[1];
		String links = "";

		if (tokens.length > 2) {
			links = tokens[2];
			final String[] linksArray = links.split("]]");
			int nrLinks = linksArray.length;
			double p = (double)Double.parseDouble(initialPageRank)/nrLinks;

			// pass PageRank mass to neighbors
			for (final String link : linksArray) {
				outputKey.set(link);
				outputValue.set(String.valueOf(p));
				context.write(outputKey, outputValue);
			}
		}

		// pass along graph structure
		outputKey.set(title);
		outputValue.set("-structure-" + links);
		context.write(outputKey, outputValue);
	}
}