package com.orienit.kalyan.hadoop.training.ngram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramKeyMapper extends Mapper<LongWritable, Text, NGramKey, LongWritable> {

	private int ngramcount;

	protected void setup(Context context) throws IOException, InterruptedException {
		ngramcount = context.getConfiguration().getInt("ngramcount", 0);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1.read the line
		String line = value.toString();

		// 2.split the line into words
		String[] words = line.split(" ");

		// 3.assign count(1) to two conscutive words
		for (int i = 0; i < words.length; i++) {
			List<String> nwords = new ArrayList<String>();

			if (i + ngramcount <= words.length) {
				for (int j = i; j < i + ngramcount; j++) {
					nwords.add(words[j]);
				}
				NGramKey nGramKey = new NGramKey();
				nGramKey.setWords(nwords);
				context.write(nGramKey, new LongWritable(1));
			}
		}
	}
}
