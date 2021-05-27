package it.unipi.cc.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.WritableComparator;

/**
 * PageRank Driver.
 */
public class Driver 
{
	/**
	 * 
	 */
	public static class NodesCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
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

	/**
	 *
	 */
	public static class NodesCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
        private final IntWritable outputValue = new IntWritable();

        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
		{
			int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
	}

	/**
	 * Parses each line of the input .xml file extracting the content of the
	 */
	public static class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text>
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

	/**
	 *
	 */
	public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text>
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

	/**
	 * 
	 */
	public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>
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

	/**
	 *
	 */
	public static class PageRankReducer extends Reducer<Text, Text, Text, Text>
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

	/**
	 * 
	 */
	public static class SorterMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
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

	/**
	 *
	 */
	public static class SorterReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
	{
        public void reduce(final DoubleWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
		{
			for (final Text title : values) {
				context.write(title, key);
			}
        }
	}

	public static class DescendingDoubleWritableComparator extends WritableComparator {
	    protected DescendingDoubleWritableComparator() {
	        super(DoubleWritable.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	        DoubleWritable key1 = (DoubleWritable) w1;
	        DoubleWritable key2 = (DoubleWritable) w2;          
	        return -1 * key1.compareTo(key2);
	    }
	}

	/**
	 * Entry point.
	 */
    public static void main(final String[] args) throws Exception
    {
    	// jobs common configuration
		final Configuration conf = new Configuration();
        final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // retrieve iterations number from command line args
        final int iterations = Integer.parseInt(otherArgs[0]);
        final float alfa = Float.parseFloat(otherArgs[1]);

        // check if the given command line arguments are enough
        if (otherArgs.length != 6+iterations) {
        	System.err.println("Usage: PageRank <iterations> <alfa> <input> <output-NodesCounter> <output-GraphBuilder> <output-PageRank> <output-Sorter>");
        	System.exit(1);
        }

        // set alfa value in the configuration for the jobs
		conf.set("ALFA", String.valueOf(alfa));

		// job0: counts the graph nodes starting from the .xml input file
    	final Job job0 = Job.getInstance(conf, "PageRank-NodesCounter");
		job0.setJarByClass(Driver.class);
		job0.setMapperClass(NodesCounterMapper.class);
		job0.setReducerClass(NodesCounterReducer.class);

		// set reducer output key/value classes
		job0.setOutputKeyClass(Text.class);
		job0.setOutputValueClass(IntWritable.class);

		// set job input format class
		job0.setInputFormatClass(TextInputFormat.class);

		// add input/output files
		FileInputFormat.addInputPath(job0, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job0, new Path(otherArgs[3]));

		// wait for job0 completion
		job0.waitForCompletion(true);

		// read nodes count from job0 output
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(otherArgs[3] + "/part-r-00000");
		if (!fs.exists(inFile)) {
			System.out.println("Input file not found");
			throw new IOException("Input file not found");
		}
		// open and read from file
		FSDataInputStream inputStream = fs.open(inFile);
		String line = inputStream.readLine();
		String[] nodesCount = line.split("\\t");

		// set nodes count in configuration for job1
		conf.set("N", nodesCount[1].trim());

		// job1: builds the graph starting from the .xml input file
    	final Job job1 = Job.getInstance(conf, "PageRank-GraphBuilder");
		job1.setJarByClass(Driver.class);
		job1.setMapperClass(GraphBuilderMapper.class);
		job1.setReducerClass(GraphBuilderReducer.class);

		// set reducer output key/value classes
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// set job input format class
		job1.setInputFormatClass(TextInputFormat.class);

		// add input/output files
		FileInputFormat.addInputPath(job1, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[4]));

		// wait for job1 completion
		job1.waitForCompletion(true);

		for (int i = 0; i < iterations; i++) {
			// job2: builds the graph starting from the .xml input file
	    	final Job job2 = Job.getInstance(conf, "PageRank-PageRank");
			job2.setJarByClass(Driver.class);
			job2.setMapperClass(PageRankMapper.class);
			job2.setReducerClass(PageRankReducer.class);

			// set reducer output key/value classes
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			// set job input format class
			job2.setInputFormatClass(TextInputFormat.class);

			// add input/output files
			FileInputFormat.addInputPath(job2, new Path(otherArgs[4 + i]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5 + i]));

			// wait for job2 completion
			job2.waitForCompletion(true);
		}

		// job3: builds the graph starting from the .xml input file
    	final Job job3 = Job.getInstance(conf, "PageRank-Sorter");
		job3.setJarByClass(Driver.class);
		job3.setMapperClass(SorterMapper.class);
		job3.setReducerClass(SorterReducer.class);

		// set mapper output key/value classes
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);

		// set reducer output key/value classes
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);

		// set job input format class
		job3.setInputFormatClass(TextInputFormat.class);

		// add input/output files
		FileInputFormat.addInputPath(job3, new Path(otherArgs[4 + iterations]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[5 + iterations]));

		// sort PageRank value in descending order
		job3.setSortComparatorClass(DescendingDoubleWritableComparator.class);

		// wait for job3 completion
		job3.waitForCompletion(true);
    }
}
