package com.orienit.kalyan.hadoop.training.xml;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KalyanXmlOutputFormat<K, V> extends FileOutputFormat<K, V> {
	public static final String XML_RECORD_TAG_NAME = "xml.record.tagname";
	public static final String XML_KEY_TAG_NAME = "xml.key.tagname";
	public static final String XML_VALUE_TAG_NAME = "xml.value.tagname";
	public static final String XML_NEW_LINE = "xml.newline";

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		Path file = getDefaultWorkFile(job, ".xml");
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileout = fs.create(file, false);
		return new KalyanXmlRecordWriter<K, V>(fileout, job);
	}

	static class KalyanXmlRecordWriter<K, V> extends RecordWriter<K, V> {
		private static final String utf8 = "UTF-8";
		protected DataOutputStream out;
		private String recordTag;
		private String keyTag;
		private String valueTag;
		private boolean newLine;

		public KalyanXmlRecordWriter(DataOutputStream out, TaskAttemptContext job) throws IOException {
			this.out = out;
			out.writeBytes("<kalyan>\n");

			Configuration conf = job.getConfiguration();
			recordTag = conf.get(XML_RECORD_TAG_NAME, "record");
			keyTag = conf.get(XML_KEY_TAG_NAME, "key");
			valueTag = conf.get(XML_VALUE_TAG_NAME, "value");
			newLine = conf.getBoolean(XML_NEW_LINE, true);
		}

		@Override
		public synchronized void write(K key, V value) throws IOException {
			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullKey && nullValue) {
				return;
			}

			if (newLine)
				out.writeBytes("\n");

			out.writeBytes("<" + recordTag + ">\n");
			if (!nullKey) {
				writeStyle(keyTag, key);
			}
			if (!nullValue) {
				writeStyle(valueTag, value);
			}
			out.writeBytes("</" + recordTag + ">\n");
		}

		private void writeStyle(String xml_tag, Object tag_value) throws IOException {
			out.writeBytes("<" + xml_tag + ">" + tag_value + "</" + xml_tag + ">\n");
		}

		@Override
		public synchronized void close(TaskAttemptContext context) throws IOException {
			if (newLine)
				out.writeBytes("\n");

			out.writeBytes("</kalyan>\n");
			out.close();
		}

		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

	}
}