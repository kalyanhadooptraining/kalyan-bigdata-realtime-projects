package com.orienit.kalyan.hadoop.training.ngram;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramKeyReducer extends Reducer<NGramKey, LongWritable, NGramKey, LongWritable> {
	@Override
	protected void reduce(NGramKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		// 1.sum the list of values
		long sum = 0;
		for (LongWritable value : values) {
			sum = sum + value.get();
		}

		// 2.assign sum to the corresponding word
		context.write(key, new LongWritable(sum));

	}
}
