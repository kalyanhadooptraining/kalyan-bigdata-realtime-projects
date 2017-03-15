package com.orienit.kalyan.hadoop.training.imageprocessing;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class KalyanImageToJpegInputFormat extends FileInputFormat<Text, KalyanImageToJpegWritable> {

	@Override
	public RecordReader<Text, KalyanImageToJpegWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new ImageToJpegRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
}

class ImageToJpegRecordReader extends RecordReader<Text, KalyanImageToJpegWritable> {

	private FSDataInputStream fileIn;
	public BufferedImage buffer = null;
	// Image information
	public String fileName = null;

	// Key/Value pair
	private Text key = null;
	private KalyanImageToJpegWritable value = null;

	// Current split
	float currentSplit = 0.0f;

	@Override
	public void close() throws IOException {
		// fileIn.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(fileName);
	}

	@Override
	public KalyanImageToJpegWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return currentSplit;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext job) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration conf = job.getConfiguration();

		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
		fileIn = fs.open(split.getPath());

		fileName = split.getPath().getName().toString();
		buffer = ImageIO.read(fileIn);
		
		value = new KalyanImageToJpegWritable(buffer);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new Text(fileName);
			return true;
		}
		return false;
	}

}
