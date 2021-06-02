package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Parses the Hyperlink Graph with the final PageRank values line by line. For
 * each node, the title and the PageRank values are parsed. The PageRank is
 * emitted as key while the title is emitted as value. This allows for exploiting
 * the built-in shuffle and sort functionality. The outlinks are discarded.
 * 
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class SorterMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
{
	private final DoubleWritable outputKey = new DoubleWritable();
	private final Text outputValue = new Text();

	@Override
	public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		final String valueString = value.toString();
		final String[] tokens = valueString.split("\\t");

		final String title = tokens[0];
		final String pr = tokens[1];

		outputKey.set(Double.parseDouble(pr));
		outputValue.set(title);
		context.write(outputKey, outputValue);
	}
}