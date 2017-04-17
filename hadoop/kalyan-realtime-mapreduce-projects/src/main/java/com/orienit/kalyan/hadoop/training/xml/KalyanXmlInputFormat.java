package com.orienit.kalyan.hadoop.training.xml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class KalyanXmlInputFormat extends TextInputFormat {
	public static final String XML_TAG_NAME = "xml.tagname";
	public static final String XML_TAG_NAME_INCLUDE = "xml.tagname.include";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new XmlRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer;

		private boolean tagInclude;
		private String START_TAG_NAME;
		private String END_TAG_NAME;

		LongWritable key;
		Text value;

		@Override
		public void initialize(InputSplit insplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			tagInclude = conf.getBoolean(XML_TAG_NAME_INCLUDE, true);
			String tagName = conf.get(XML_TAG_NAME);
			START_TAG_NAME = "<" + tagName + ">";
			END_TAG_NAME = "</" + tagName + ">";
			startTag = START_TAG_NAME.getBytes("utf-8");
			endTag = END_TAG_NAME.getBytes("utf-8");
			buffer = new DataOutputBuffer();

			FileSplit split = (FileSplit) insplit;
			// open the file and seek to the start of the split
			start = split.getStart();
			end = start + split.getLength();

			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(conf);
			fsin = fs.open(split.getPath());
			fsin.seek(start);

			key = new LongWritable();
			value = new Text();
		}

		@Override
		public void close() throws IOException {
			fsin.close();
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
			// float percentage = ((start + pos) / (end- start) );
			// return percentage;
			return 0;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					long mystart = fsin.getPos() - startTag.length;
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							key.set(mystart);
							value.set(buffer.getData(), 0, buffer.getLength());
							if (!tagInclude) {
								String oldValue = value.toString();
								String newValue = oldValue.replace(START_TAG_NAME, "").replace(END_TAG_NAME, "");
								value.clear();
								value.set(newValue);
							}
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();

				// end of file:
				if (b == -1)
					return false;

				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;

				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}
	}
}
