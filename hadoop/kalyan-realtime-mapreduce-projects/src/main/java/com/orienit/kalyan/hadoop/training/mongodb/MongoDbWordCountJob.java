package com.orienit.kalyan.hadoop.training.mongodb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoDbWordCountJob implements Tool {
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
		wordCountJob.setJobName("Orien IT MongoDb WordCount Job");

		// to call this as a jar
		wordCountJob.setJarByClass(this.getClass());

		// setting custom mapper class
		wordCountJob.setMapperClass(MongoDbWordCountMapper.class);

		// setting custom reducer class
		wordCountJob.setReducerClass(MongoDbWordCountReducer.class);

		// setting custom combiner class
		// wordCountJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// wordCountJob.setNumReduceTasks(26);

		// setting custom partitioner class
		// wordCountJob.setPartitionerClass(WordCountPartitioner.class);

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
		int status = ToolRunner.run(new Configuration(), new MongoDbWordCountJob(), args);
		System.out.println("My Status: " + status);
	}
}
