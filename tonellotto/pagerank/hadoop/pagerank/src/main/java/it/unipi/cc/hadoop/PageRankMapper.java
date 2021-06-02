package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Parses the Hyperlink Graph input file line by line. The node title and initial
 * PageRank values are first retrieved. The contribution to the outlinks is
 * computed. For each of the outlinks of the node the contribution is emitted.
 * Finally the graph structure is also passed along for the next iteration.
 * 
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private final Text outputKey = new Text();
	private final Text outputValue = new Text();

	@Override
	public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		// parse node page title and initial PageRank
		final String line = value.toString();
		final String tokens[] = line.split("\\t");
		final String title = tokens[0];
		final String initialPageRank = tokens[1];
		String links = "";

		// if outlinks are available, compute contribution
		if (tokens.length > 2) {
			links = tokens[2];
			final String[] linksArray = links.split("]]");
			int nrLinks = linksArray.length;
			double contribution = (double)Double.parseDouble(initialPageRank)/nrLinks;

			// pass PageRank mass to outlinks
			for (final String link : linksArray) {
				outputKey.set(link);
				outputValue.set(String.valueOf(contribution));
				context.write(outputKey, outputValue);
			}
		}

		// pass along graph structure
		outputKey.set(title);
		outputValue.set("-structure-" + links);
		context.write(outputKey, outputValue);
	}
}