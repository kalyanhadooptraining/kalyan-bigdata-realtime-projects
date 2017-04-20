package com.orienit.kalyan.hadoop.training.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseMRJob implements Tool {
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
		Job job = new Job(config);

		// setting the job name
		job.setJobName("Orien IT Hbase MR Job");

		// to call this as a jar
		job.setJarByClass(this.getClass());

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		String inputTable = "input";
		String outputTable = "output";

		// setting custom mapper details
		TableMapReduceUtil.initTableMapperJob(inputTable, scan, HbaseMRMapper.class, Text.class, LongWritable.class,
				job);

		// setting custom reducer details
		TableMapReduceUtil.initTableReducerJob(outputTable, HbaseMRReducer.class, job);

		// setting the input format class ,i.e for K1, V1
		job.setInputFormatClass(TableInputFormat.class);

		// setting the output format class
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);

		// to execute the job and return the status
		return job.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new HbaseMRJob(), args);
		System.out.println("My Status: " + status);
	}
}

class HbaseMRMapper extends TableMapper<Text, LongWritable> {
	public static final byte[] CF = "cf".getBytes();
	public static final byte[] ATTR = "record".getBytes();

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		// Read the line
		String line = new String(value.getValue(CF, ATTR));

		// Split the line into words
		String[] words = line.split(" ");

		// assign count(1) to each word
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
	}
}

class HbaseMRReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
	public static final byte[] CF = "cf".getBytes();
	public static final byte[] COUNT = "count".getBytes();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		// Sum the list of values
		long sum = 0;
		for (LongWritable value : values) {
			sum = sum + value.get();
		}

		// assign sum to corresponding word
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(CF, COUNT, Bytes.toBytes(String.valueOf(sum)));
		context.write(null, put);
	}
}
