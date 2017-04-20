package com.orienit.kalyan.hadoop.training.multipleinputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultipleInputsJob implements Tool {
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
		Job job = new Job(getConf());

		// setting the job name
		job.setJobName("Orien IT WordCount Job");

		// to call this as a jar
		job.setJarByClass(this.getClass());

		// setting custom mapper class
		// job.setMapperClass(WordCountMapper.class);

		// setting custom reducer class
		job.setReducerClass(MultipleInputsReducer.class);

		// setting mapper output key class: K2
		job.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		job.setMapOutputValueClass(LongWritable.class);

		// setting reducer output key class: K3
		job.setOutputKeyClass(Text.class);

		// setting reducer output value class: V3
		job.setOutputValueClass(LongWritable.class);

		// setting the input format class ,i.e for K1, V1
		// job.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MultipleInputsSpaceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), KeyValueTextInputFormat.class,
				MultipleInputsTabMapper.class);

		// setting the input file path
		// FileInputFormat.addInputPath(job, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		Path outputpath = new Path(args[2]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return job.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations
		int status = ToolRunner.run(new Configuration(), new MultipleInputsJob(), args);
		System.out.println("My Status: " + status);
	}
}

class MultipleInputsSpaceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
	};
}

class MultipleInputsTabMapper extends Mapper<Text, Text, Text, LongWritable> {
	@Override
	protected void map(Text key, Text value, Context context) throws java.io.IOException, InterruptedException {
		context.write(key, new LongWritable(Long.parseLong(value.toString())));
	};
}

class MultipleInputsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> value, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		while (value.iterator().hasNext()) {
			sum += value.iterator().next().get();
		}
		context.write(key, new LongWritable(sum));
	};
}
