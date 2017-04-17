package com.orienit.kalyan.hadoop.training.multipleinputs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountSpaceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
	};
}
