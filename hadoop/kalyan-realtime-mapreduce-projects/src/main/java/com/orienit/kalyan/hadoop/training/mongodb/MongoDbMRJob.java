package com.orienit.kalyan.hadoop.training.mongodb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoDbMRJob implements Tool {
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
		wordCountJob.setJobName("Orien IT MongoDb MR Job");

		// to call this as a jar
		wordCountJob.setJarByClass(this.getClass());

		// setting custom mapper class
		wordCountJob.setMapperClass(MongoDbMRMapper.class);

		// setting custom reducer class
		wordCountJob.setReducerClass(MongoDbMRReducer.class);

		// setting mapper output key class: K2
		wordCountJob.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		wordCountJob.setMapOutputValueClass(LongWritable.class);

		// setting reducer output key class: K3
		wordCountJob.setOutputKeyClass(Text.class);

		// setting reducer output value class: V3
		wordCountJob.setOutputValueClass(LongWritable.class);

		// setting the input format class ,i.e for K1, V1
		wordCountJob.setInputFormatClass(MongoInputFormat.class);

		// setting the output format class
		wordCountJob.setOutputFormatClass(MongoOutputFormat.class);

		final Configuration conf = getConf();

		// setting the input file path
		// FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
		// MongoConfigUtil.setInputURI(conf,
		// "mongodb://localhost/kalyan.input");
		MongoConfigUtil.setInputURI(conf, args[0]);

		// setting the output folder path
		// FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
		// MongoConfigUtil.setOutputURI(conf,
		// "mongodb://localhost/kalyan.output");
		MongoConfigUtil.setOutputURI(conf, args[1]);

		// to execute the job and return the status
		return wordCountJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new MongoDbMRJob(), args);
		System.out.println("My Status: " + status);
	}
}

class MongoDbMRMapper extends Mapper<Object, BSONObject, Text, LongWritable> {
	@Override
	protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
		// Read the line
		String line = value.get("record").toString();

		// Split the line into words
		String[] words = line.split(" ");

		// assign count(1) to each word
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
	}
}

class MongoDbMRReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		// Sum the list of values
		long sum = 0;
		for (LongWritable value : values) {
			sum = sum + value.get();
		}

		// assign sum to corresponding word
		context.write(key, new LongWritable(sum));
	}
}
