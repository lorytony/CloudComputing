package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This Mapper is used by the job in charge of counting the number of nodes of the
 * hyperlink graph. The map() method is called once for each of the lines in the
 * input .xml file. Whenever a <title> tag is found, this is a node. The fixed
 * key N is outputted with the values aggregated by an intermediate In-Mapper
 * combiner.
 * 
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class NodesCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final Text outputKey = new Text("N");
	private final IntWritable outputValue = new IntWritable();

	// in-mapper combiner aggregator
	private int intermediateSum = 0;

	@Override
	public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		final String page = value.toString();

		// find <title></title> tag
		int titleStart = page.indexOf("<title>");
		if (titleStart >= 0) {
			int titleEnd = page.indexOf("</title>", titleStart);
			if (titleEnd >= 0) {
				intermediateSum += 1;
			}
		}
	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException
	{
		outputValue.set(intermediateSum);
		context.write(outputKey, outputValue);
	}
}