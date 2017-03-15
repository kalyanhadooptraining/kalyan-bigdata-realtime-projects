package com.orienit.kalyan.hadoop.training.pdf.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

public class KalyanPdfInputFormat extends FileInputFormat<LongWritable, Text> {
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter)
			throws IOException {
		return new PdfRecordReader(jobConf, split);
	}
}

class PdfRecordReader implements RecordReader<LongWritable, Text> {
	private FSDataInputStream fileIn;
	private String[] lines = null;
	private LongWritable key = null;
	private Text value = null;
	private PDDocument pdf = null;

	public PdfRecordReader(Configuration conf, InputSplit inputSplit) throws IOException{
		FileSplit split = (FileSplit) inputSplit;
		final Path file = split.getPath();

		/*
		 * The below code contains the logic for opening the file and seek to
		 * the start of the split. Here we are applying the Pdf Parsing logic
		 */
		FileSystem fs = file.getFileSystem(conf);
		fileIn = fs.open(split.getPath());
		pdf = PDDocument.load(fileIn);
		PDFTextStripper stripper = new PDFTextStripper();
		String parsedText = stripper.getText(pdf);
		this.lines = parsedText.split("\n");
		key = new LongWritable();
		value = new Text();
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
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
	public LongWritable createKey() {
		return key;
	}

	@Override
	public Text createValue() {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		pdf.close();
		fileIn.close();
	}

	@Override
	public long getPos() throws IOException {
		return 0;
	}

}
