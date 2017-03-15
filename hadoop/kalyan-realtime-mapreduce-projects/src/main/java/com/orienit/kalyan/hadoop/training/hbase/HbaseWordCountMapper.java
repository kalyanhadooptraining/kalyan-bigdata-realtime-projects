package com.orienit.kalyan.hadoop.training.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class HbaseWordCountMapper extends TableMapper<Text, LongWritable> {
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
