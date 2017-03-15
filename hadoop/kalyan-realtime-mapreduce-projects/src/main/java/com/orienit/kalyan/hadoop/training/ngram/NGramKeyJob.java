package com.orienit.kalyan.hadoop.training.ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NGramKeyJob implements Tool {
	// Initializing configuration object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf; // getting the configuration
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf; // setting the configuration
	}

	@Override
	public int run(String[] args) throws Exception {

		// initializing the job configuration
		Job nGramKeyJob = new Job(getConf());

		// setting the job name
		nGramKeyJob.setJobName("Orien IT NGramKey Job");

		// to call this as a jar
		nGramKeyJob.setJarByClass(this.getClass());

		// setting custom mapper class
		nGramKeyJob.setMapperClass(NGramKeyMapper.class);

		// setting custom reducer class
		nGramKeyJob.setReducerClass(NGramKeyReducer.class);

		// setting custom combiner class
		// biaGramJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// wordCountJob.setNumReduceTasks(26);

		// setting custom partitioner class
		// biaGramJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2
		nGramKeyJob.setMapOutputKeyClass(NGramKey.class);

		// setting mapper output value class: V2
		nGramKeyJob.setMapOutputValueClass(LongWritable.class);

		// setting reducer output key class: K3
		nGramKeyJob.setOutputKeyClass(NGramKey.class);

		// setting reducer output value class: V3
		nGramKeyJob.setOutputValueClass(LongWritable.class);

		// setting the input format class ,i.e for K1, V1
		nGramKeyJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		nGramKeyJob.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(nGramKeyJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(nGramKeyJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return nGramKeyJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations
		Configuration conf = new Configuration();
		conf.set("ngramcount", args[2]);

		int status = ToolRunner.run(conf, new NGramKeyJob(), args);
		System.out.println("My Status: " + status);
	}

}
