package com.orienit.kalyan.hadoop.training.pdf.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.itextpdf.text.Chunk;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfWriter;

public class KalyanPdfOutputFormat<K, V> extends FileOutputFormat<K, V> {
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		String keyValueSeparator = conf.get("mapred.pdfoutputformat.separator", "\t");
		Path file = getDefaultWorkFile(job, "");
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		return new KalyanPdfRecordWriter<K, V>(fileOut, keyValueSeparator);
	}
}

class KalyanPdfRecordWriter<K, V> extends RecordWriter<K, V> {
	protected DataOutputStream out;
	private final String keyValueSeparator;
	private Document document;
	private PdfWriter writer;

	public KalyanPdfRecordWriter(DataOutputStream out, String keyValueSeparator) throws UnsupportedEncodingException {
		this.out = out;
		this.keyValueSeparator = keyValueSeparator;

		document = new Document();
		try {
			writer = PdfWriter.getInstance(document, out);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		document.open();
	}

	public KalyanPdfRecordWriter(DataOutputStream out) throws UnsupportedEncodingException {
		this(out, "\t");
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		try {
			boolean nullKey = (key == null) || (key instanceof NullWritable);
			boolean nullValue = (value == null) || (value instanceof NullWritable);
			if ((nullKey) && (nullValue)) {
				return;
			}
			String result = "";
			if (!(nullKey) && !(nullValue)) {
				if (this.keyValueSeparator.equals("\t")) {
					result = key.toString() + Chunk.TABBING + value.toString();
				} else if (this.keyValueSeparator.equals("\n")) {
					result = key.toString() + Chunk.NEWLINE + value.toString();
				} else {
					result = key.toString() + this.keyValueSeparator + value.toString();
				}
			} else if (!(nullKey) && (nullValue)) {
				result = key.toString();
			} else if ((nullKey) && !(nullValue)) {
				result = value.toString();
			}
			document.add(new Paragraph(result));
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close(TaskAttemptContext job) throws IOException, InterruptedException {
		document.close();
		writer.close();
		out.close();
	}
}