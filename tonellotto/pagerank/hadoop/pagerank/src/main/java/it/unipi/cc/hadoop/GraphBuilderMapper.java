package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Parses each line of the input .xml file extracting the content of the
 */
public class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private final Text outputKey = new Text();
	private final Text outputValue = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		final String page = value.toString();

		// find <title></title> tag
		int titleStart = page.indexOf("<title>");
		if (titleStart >= 0) {
			int titleEnd = page.indexOf("</title>");
			outputKey.set(page.substring(titleStart + 7, titleEnd));

			// find and extract <text></text> tag
			final int textStartOpen = page.indexOf("<text");
			final int textStartClose = page.indexOf(">", textStartOpen);
			final int textEnd = page.indexOf("</text>", textStartClose);
			final String body = page.substring(textStartClose + 1, textEnd);

			// search for links [[]] inside <text></text>
			int linkStart = body.indexOf("[[");
			while (linkStart >= 0) {
				int linkEnd = body.indexOf("]]", linkStart);
				outputValue.set(body.substring(linkStart + 2, linkEnd));
				linkStart = body.indexOf("[[", linkEnd + 1);

				context.write(outputKey, outputValue);
			}
		}
	}
}