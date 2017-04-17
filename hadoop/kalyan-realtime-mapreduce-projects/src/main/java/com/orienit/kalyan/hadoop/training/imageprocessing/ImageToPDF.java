package com.orienit.kalyan.hadoop.training.imageprocessing;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImageToPDF extends Configured implements Tool {
	public static class PDFMapper extends Mapper<Text, KalyanImageToPdfWritable, Text, KalyanImageToPdfWritable> {

		private static final Log log = LogFactory.getLog(PDFMapper.class);
		String dirName = null;
		String fileName = null;

		@Override
		public void map(Text key, KalyanImageToPdfWritable value, Context context)
				throws IOException, InterruptedException {
			try {
				for (int i = 0; i < value.bufferList.size(); i++) {
					dirName = value.dirList.get(i).substring(43, value.dirList.get(i).length());
					fileName = value.keyList.get(i).substring(0, value.keyList.get(i).length() - 4);
					context.write(new Text(fileName), value);
				}
			} catch (Exception e) {
				log.info(e);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ImageToPDF <in> <out>");
			System.exit(2);
		}
		Job job = new Job(getConf(), "ImageToPDF");
		job.setJarByClass(ImageToPDF.class);
		job.setMapperClass(PDFMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KalyanImageToPdfWritable.class);
		job.setInputFormatClass(KalyanImageToPdfInputFormat.class);
		job.setOutputFormatClass(KalyanImageToPdfOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(getConf()).delete(new Path(otherArgs[1]), true);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ImageToPDF(), args);
	}
}
