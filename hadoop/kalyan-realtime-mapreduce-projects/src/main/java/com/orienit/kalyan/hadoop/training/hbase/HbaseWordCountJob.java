package com.orienit.kalyan.hadoop.training.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseWordCountJob implements Tool {
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

		// initializing the hbase configuration object
		Configuration config = HBaseConfiguration.create(getConf());

		// initializing the job configuration
		Job wordCountJob = new Job(config);

		// setting the job name
		wordCountJob.setJobName("Orien IT Hbase WordCount Job");

		// to call this as a jar
		wordCountJob.setJarByClass(this.getClass());

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		String inputTable = "input";
		String outputTable = "output";

		// setting custom mapper details
		TableMapReduceUtil.initTableMapperJob(inputTable, scan, HbaseWordCountMapper.class, Text.class,
				LongWritable.class, wordCountJob);

		// setting custom reducer details
		TableMapReduceUtil.initTableReducerJob(outputTable, HbaseWordCountReducer.class, wordCountJob);

		// setting the input format class ,i.e for K1, V1
		wordCountJob.setInputFormatClass(TableInputFormat.class);

		// setting the output format class
		wordCountJob.setOutputFormatClass(TableOutputFormat.class);
		wordCountJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);

		// to execute the job and return the status
		return wordCountJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new HbaseWordCountJob(), args);
		System.out.println("My Status: " + status);
	}
}
