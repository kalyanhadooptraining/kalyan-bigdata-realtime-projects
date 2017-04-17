package com.orienit.kalyan.hadoop.training.invertedindexing;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexingReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values, Context context)
			throws java.io.IOException, InterruptedException {
		String removeDuplicates = removeDuplicates(values);
		context.write(key, new Text(removeDuplicates));
	};

	private static String removeDuplicates(java.lang.Iterable<Text> files) {
		Set<String> uniqueFiles = new LinkedHashSet<String>();
		for (Text file : files) {
			uniqueFiles.add(file.toString());
		}
		String allfiles = StringUtils.join(uniqueFiles, ",");
		return allfiles;
	}
}
