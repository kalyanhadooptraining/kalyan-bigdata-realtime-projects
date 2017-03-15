package com.orienit.kalyan.hadoop.training.invertedindexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexingJob implements Tool {

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		Job invertedIndexingJob = new Job(getConf());
		invertedIndexingJob.setJobName("OrienIT Inverted indexing");
		invertedIndexingJob.setJarByClass(this.getClass());

		invertedIndexingJob.setMapperClass(InvertedIndexingMapper.class);
		invertedIndexingJob.setReducerClass(InvertedIndexingReducer.class);

		invertedIndexingJob.setMapOutputKeyClass(Text.class);
		invertedIndexingJob.setMapOutputValueClass(Text.class);

		invertedIndexingJob.setOutputKeyClass(Text.class);
		invertedIndexingJob.setOutputValueClass(Text.class);

		invertedIndexingJob.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.setInputPaths(invertedIndexingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(invertedIndexingJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);

		return invertedIndexingJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new InvertedIndexingJob(), args);
	}

}












