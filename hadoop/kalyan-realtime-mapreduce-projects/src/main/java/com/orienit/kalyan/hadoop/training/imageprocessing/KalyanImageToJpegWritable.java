package com.orienit.kalyan.hadoop.training.imageprocessing;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.Writable;

public class KalyanImageToJpegWritable implements Writable {

	public BufferedImage buffer;

	public KalyanImageToJpegWritable() {
	}

	public KalyanImageToJpegWritable(BufferedImage buff) {
		this.buffer = buff;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		buffer = ImageIO.read(new BufferedInputStream((InputStream) in));
	}

	public void write(DataOutput out) throws IOException {

		ImageIO.write(buffer, "jpeg", (OutputStream) out);
	}
}
