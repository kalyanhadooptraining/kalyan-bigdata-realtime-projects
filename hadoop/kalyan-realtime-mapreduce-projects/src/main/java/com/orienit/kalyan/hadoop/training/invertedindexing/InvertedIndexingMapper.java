package com.orienit.kalyan.hadoop.training.invertedindexing;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexingMapper extends Mapper<Text, BytesWritable, Text, Text> {

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws java.io.IOException, InterruptedException {
		String fileName = key.toString();
		String fileContent = new String(value.getBytes(), 0, value.getLength());
		String[] words = fileContent.split(" ");
		for (String word : words) {
			if (word.contains("\n")) {
				String[] nwords = word.split("\n");
				for (String nword : nwords) {
					context.write(new Text(nword), new Text(fileName));
				}
			} else
				context.write(new Text(word), new Text(fileName));
		}
	};
}








