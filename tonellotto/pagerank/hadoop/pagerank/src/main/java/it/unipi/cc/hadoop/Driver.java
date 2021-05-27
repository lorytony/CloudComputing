package it.unipi.cc.hadoop;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Hadoop Driver.
 * 
 * @author Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella, Rambod Rahmani.
 */
public class Driver 
{
	/**
	 * Entry point.
	 * 
	 * @param args command line arguments.
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
