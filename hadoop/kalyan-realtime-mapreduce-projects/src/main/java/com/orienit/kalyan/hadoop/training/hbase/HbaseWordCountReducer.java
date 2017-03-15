package com.orienit.kalyan.hadoop.training.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class HbaseWordCountReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
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
