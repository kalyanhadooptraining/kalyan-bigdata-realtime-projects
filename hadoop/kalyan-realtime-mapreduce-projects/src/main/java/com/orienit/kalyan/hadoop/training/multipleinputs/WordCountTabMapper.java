package com.orienit.kalyan.hadoop.training.multipleinputs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountTabMapper extends Mapper<Text, Text, Text, LongWritable> {
	@Override
	protected void map(Text key, Text value, Context context) throws java.io.IOException, InterruptedException {
		context.write(key, new LongWritable(Long.parseLong(value.toString())));
	};
}