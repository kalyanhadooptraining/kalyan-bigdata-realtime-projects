package com.orienit.kalyan.hadoop.training.multipleinputs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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
