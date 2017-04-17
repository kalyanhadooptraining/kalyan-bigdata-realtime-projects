package com.orienit.kalyan.hadoop.training.mongodb;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class MongoDbWordCountMapper extends Mapper<Object, BSONObject, Text, LongWritable> {
	@Override
	protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
		// Read the line
		String line = value.get("record").toString();

		// Split the line into words
		String[] words = line.split(" ");

		// assign count(1) to each word
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
	}
}
