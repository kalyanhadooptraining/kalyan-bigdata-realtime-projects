package com.orienit.kalyan.hadoop.training.imageprocessing;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.itextpdf.text.Document;
import com.itextpdf.text.pdf.PdfCopy;
import com.itextpdf.text.pdf.PdfReader;

public class KalyanImageToPdfOutputFormat extends FileOutputFormat<Text, KalyanImageToPdfWritable> {
	TaskAttemptContext job;

	@Override
	public RecordWriter<Text, KalyanImageToPdfWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		this.job = job;
		return new ImageToPdfRecordWriter(job);
	}

	public Path extracted(TaskAttemptContext job, String path) throws IOException {
		return getDefaultWorkFile(job, path);
	}

}

class ImageToPdfRecordWriter extends RecordWriter<Text, KalyanImageToPdfWritable> {
	private final Log log = LogFactory.getLog(ImageToPdfRecordWriter.class);
	TaskAttemptContext job;
	Path file;
	FileSystem fs;
	int i = 0;

	ImageToPdfRecordWriter(TaskAttemptContext job) {
		this.job = job;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException {
		// doc.close();
	}

	// get the names of the image and directories and pass them as output
	@Override
	public synchronized void write(Text key, KalyanImageToPdfWritable value) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		KalyanImageToPdfOutputFormat ios = new KalyanImageToPdfOutputFormat();
		Path name = ios.extracted(job, null);
		String outfilepath = name.toString();
		String keyname = key.toString();
		Path file = new Path((outfilepath.substring(0, outfilepath.length() - 16)) + keyname);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		writeDocument(value, fileOut);

	}

	// write the pdf files passed by reader
	public void writeDocument(KalyanImageToPdfWritable o, FSDataOutputStream out) throws IOException {
		try {
			Document doc = new Document();
			PdfCopy copy = new PdfCopy(doc, (OutputStream) out);

			int inc = 0;
			PdfReader reader = new PdfReader(o.bytes);
			log.info(reader.getFileLength());
			doc.open();
			while (inc < reader.getNumberOfPages()) {
				inc++;
				copy.addPage(copy.getImportedPage(reader, 1));
			}
			doc.close();
		} catch (Exception e) {
			log.info("exception : " + e);
		}
	}
}
