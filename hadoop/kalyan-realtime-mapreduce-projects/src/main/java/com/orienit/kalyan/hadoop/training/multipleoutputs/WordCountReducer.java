package com.orienit.kalyan.hadoop.training.multipleoutputs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private MultipleOutputs<Text, LongWritable> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, LongWritable>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		// 1.sum the list of values
		long sum = 0;
		for (LongWritable value : values) {
			sum = sum + value.get();
		}

		// 2.assign sum to the corresponding word
		// context.write(key, new LongWritable(sum));

		if (sum % 2 == 0) {
			mos.write("EVEN", key, new LongWritable(sum));
		} else {
			mos.write("ODD", key, new LongWritable(sum));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}










