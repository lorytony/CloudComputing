package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 */
public class NodesCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final Text outputKey = new Text("N");
	private final IntWritable outputValue = new IntWritable(1);

	@Override
	public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		final String page = value.toString();

		// find <title></title> tag
		int titleStart = page.indexOf("<title>");
		if (titleStart >= 0) {
			int titleEnd = page.indexOf("</title>", titleStart);
			if (titleEnd >= 0) {
				context.write(outputKey, outputValue);
			}
		}
	}
}