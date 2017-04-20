package com.orienit.kalyan.hadoop.training.invertedindexing;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexingJob implements Tool {
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
		job.setJobName("Orien IT Inverted Indexing Job");

		// to call this as a jar
		job.setJarByClass(this.getClass());

		// setting custom mapper class
		job.setMapperClass(InvertedIndexingMapper.class);

		// setting custom reducer class
		job.setReducerClass(InvertedIndexingReducer.class);

		// setting mapper output key class: K2
		job.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		job.setMapOutputValueClass(Text.class);

		// setting reducer output key class: K3
		job.setOutputKeyClass(Text.class);

		// setting reducer output value class: V3
		job.setOutputValueClass(Text.class);

		// setting the input format class ,i.e for K1, V1
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// setting the output format class
		job.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return job.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new InvertedIndexingJob(), args);
		System.out.println("My Status: " + status);
	}
}

class InvertedIndexingMapper extends Mapper<Text, BytesWritable, Text, Text> {

	@Override
	protected void map(Text key, BytesWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		String fileName = key.toString();
		String fileContent = new String(value.getBytes(), 0, value.getLength());
		String[] words = fileContent.split(" ");
		for (String word : words) {
			if (word.contains("\n")) {
				String[] nwords = word.split("\n");
				for (String nword : nwords) {
					context.write(new Text(nword), new Text(fileName));
				}
			} else
				context.write(new Text(word), new Text(fileName));
		}
	};
}

class InvertedIndexingReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values, Context context)
			throws java.io.IOException, InterruptedException {
		String removeDuplicates = removeDuplicates(values);
		context.write(key, new Text(removeDuplicates));
	};

	private static String removeDuplicates(java.lang.Iterable<Text> files) {
		Set<String> uniqueFiles = new LinkedHashSet<String>();
		for (Text file : files) {
			uniqueFiles.add(file.toString());
		}
		String allfiles = StringUtils.join(uniqueFiles, ",");
		return allfiles;
	}
}
