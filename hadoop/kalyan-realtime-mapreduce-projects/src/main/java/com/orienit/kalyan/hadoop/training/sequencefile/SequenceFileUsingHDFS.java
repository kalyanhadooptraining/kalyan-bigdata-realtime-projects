package com.orienit.kalyan.hadoop.training.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileUsingHDFS implements Tool {
	// Initializing configuration object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf; // getting the configuration
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf; // setting the configuration
	}

	@Override
	public int run(String[] args) throws Exception {
		int BUFFER_SIZE = 2 * 1024 * 1024;
		Configuration conf = getConf();

		Writer writer = null;
		Path inputDir = new Path(args[0]);
		Path seqFile = new Path(args[1]);

		Text fileName = new Text();
		BytesWritable fileContent = new BytesWritable();
		byte[] data = new byte[BUFFER_SIZE];

		try {
			writer = SequenceFile.createWriter(seqFile.getFileSystem(conf), conf, seqFile, Text.class,
					BytesWritable.class, CompressionType.BLOCK);
			FileSystem fs = inputDir.getFileSystem(conf);
			for (FileStatus file : fs.listStatus(inputDir)) {
				// open the file
				FSDataInputStream stream = fs.open(file.getPath());

				// read the file content
				stream.read(data);

				// write the file content
				fileContent.set(data, 0, (int) file.getLen());

				// write the file name
				fileName.set(file.getPath().toString());

				// append the file name & file content to writer object
				writer.append(fileName, fileContent);

				// close the file
				stream.close();
			}
			writer.close();
			fs.close();
		} finally {
			if (writer != null) {
				writer.close();
			}
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SequenceFileUsingHDFS(), args);
		System.out.println("My Status: " + status);
	}
}
