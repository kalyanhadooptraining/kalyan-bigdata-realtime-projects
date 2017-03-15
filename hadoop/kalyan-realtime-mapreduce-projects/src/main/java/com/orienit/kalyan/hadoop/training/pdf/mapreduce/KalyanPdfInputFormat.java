package com.orienit.kalyan.hadoop.training.pdf.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

public class KalyanPdfInputFormat extends FileInputFormat<LongWritable, Text> {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new PdfRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}

class PdfRecordReader extends RecordReader<LongWritable, Text> {
	private FSDataInputStream fileIn;
	private String[] lines = null;
	private LongWritable key = null;
	private Text value = null;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		final Path file = split.getPath();

		/*
		 * The below code contains the logic for opening the file and seek to
		 * the start of the split. Here we are applying the Pdf Parsing logic
		 */
		FileSystem fs = file.getFileSystem(conf);
		fileIn = fs.open(split.getPath());
		PDDocument pdf = PDDocument.load(fileIn);
		PDFTextStripper stripper = new PDFTextStripper();
		String parsedText = stripper.getText(pdf);
		this.lines = parsedText.split("\n");
		key = new LongWritable();
		value = new Text();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		int temp = (int) key.get();
		if (temp < lines.length) {
			int count = (int) key.get();
			value.set(lines[count]);
			count = count + 1;
			key.set(count);
		} else {
			return false;
		}
		if (key == null || value == null) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		fileIn.close();
	}

}
