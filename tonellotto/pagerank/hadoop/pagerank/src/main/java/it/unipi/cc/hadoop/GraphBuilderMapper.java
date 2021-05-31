package it.unipi.cc.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This Mapper is used by job0 which is in charge of building the hyperlink graph.
 * It parses each line of the input .xml file extracting the content of the
 * <title> and <text> tags. The content of the <text> tag is further processed in
 * order to detect outlinks formatted as [[page name]].
 * The output uses the <title> tag content as the output key whereas the outlinks
 * detected inside the <text> tag are emitted as output value.
 * 
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private final Text outputKey = new Text();
	private final Text outputValue = new Text();

	@Override
	public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
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