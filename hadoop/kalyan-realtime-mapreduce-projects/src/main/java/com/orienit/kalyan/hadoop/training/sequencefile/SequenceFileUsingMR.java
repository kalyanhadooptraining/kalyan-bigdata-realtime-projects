package com.orienit.kalyan.hadoop.training.sequencefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileUsingMR implements Tool {
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
		Job wordCountJob = new Job(getConf());

		// setting the job name
		wordCountJob.setJobName("Orien IT SequenceFileUsingMR Job");

		// to call this as a jar
		wordCountJob.setJarByClass(this.getClass());

		// setting custom mapper class
		wordCountJob.setMapperClass(SequenceFileUsingMRMapper.class);

		// setting custom reducer class
		// wordCountJob.setReducerClass(SequenceFileUsingMRReducer.class);

		// setting mapper output key class: K2
		wordCountJob.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		wordCountJob.setMapOutputValueClass(BytesWritable.class);

		// setting reducer output key class: K2
		wordCountJob.setOutputKeyClass(Text.class);

		// setting reducer output value class: V2
		wordCountJob.setOutputValueClass(BytesWritable.class);

		// setting the input format class ,i.e for K1, V1
		wordCountJob.setInputFormatClass(WholeFileInputFormat.class);

		// setting the output format class
		wordCountJob.setOutputFormatClass(SequenceFileOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return wordCountJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SequenceFileUsingMR(), args);
		System.out.println("My Status: " + status);
	}
}

class SequenceFileUsingMRMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
	Text filePath;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		InputSplit inputSplit = context.getInputSplit();
		FileSplit fileSplit = (FileSplit) inputSplit;
		Path path = fileSplit.getPath();
		filePath = new Text(path.toString());
	}

	@Override
	protected void map(NullWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(filePath, value);
	}
}
