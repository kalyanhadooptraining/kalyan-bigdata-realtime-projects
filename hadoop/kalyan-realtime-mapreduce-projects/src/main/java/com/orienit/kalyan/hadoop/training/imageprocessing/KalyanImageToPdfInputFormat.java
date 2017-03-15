package com.orienit.kalyan.hadoop.training.imageprocessing;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//custom inputformat to read image files and store it in arrays
public class KalyanImageToPdfInputFormat extends FileInputFormat<Text, KalyanImageToPdfWritable> {

	@Override
	public RecordReader<Text, KalyanImageToPdfWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ImageToPdfRecordReader();
	}

	// are the input files splittable or not
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

}

class ImageToPdfRecordReader extends RecordReader<Text, KalyanImageToPdfWritable> {

	private static final Log LOG = LogFactory.getLog(ImageToPdfRecordReader.class);

	private FSDataInputStream fileIn;

	// Image informations
	private BufferedImage buffer = null;
	private String fileName = null;
	private ArrayList<String> name = new ArrayList<String>();
	private ArrayList<BufferedImage> bufferList = new ArrayList<BufferedImage>();
	private ArrayList<String> filedir = new ArrayList<String>();

	// Key/Value pair
	private Text key = null;
	private KalyanImageToPdfWritable value = null;

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
	public KalyanImageToPdfWritable getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return currentSplit;
	}

	// checks for directory or not, if not reads the image and adds in
	// arrays
	public void readDir(Path file, FileSplit split, Configuration conf) {
		try {
			FileSystem fs = file.getFileSystem(conf);
			FileStatus[] stats = fs.listStatus(file);
			Path[] paths = FileUtil.stat2Paths(stats);
			for (Path path : paths) {
				FileSystem fs1 = path.getFileSystem(conf);
				FileStatus stat = fs1.getFileStatus(path);
				if (stat.isDir() == false) {
					fileIn = fs1.open(new Path(path.toString()));
					fileName = path.getName().toString();
					LOG.info(fileName);
					buffer = ImageIO.read(fileIn);
					bufferList.add(buffer);
					name.add(fileName);
					filedir.add(file.toString());
				}
				if (stat.isDir() == true) {
					file = stat.getPath();
					this.readDir(file, split, conf);
				}
				value = new KalyanImageToPdfWritable(bufferList, name, filedir);
			}
		} catch (Exception e) {
			LOG.info("exception " + e);
		}

	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext job) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration conf = job.getConfiguration();
		Path file = split.getPath();
		this.readDir(file, split, conf);
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
