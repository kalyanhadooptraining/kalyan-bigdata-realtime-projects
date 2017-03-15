package com.orienit.kalyan.hadoop.training.imageprocessing;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.itextpdf.text.Document;
import com.itextpdf.text.Image;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfCopy;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfWriter;

//class to make image and pdfs serializable
public class KalyanImageToPdfWritable implements Writable {
	private static final Log log = LogFactory.getLog(KalyanImageToPdfWritable.class);
	public byte[] bytes;

	PdfReader reader = null;
	int i = 0;
	public ArrayList<BufferedImage> bufferList = new ArrayList<BufferedImage>();
	public ArrayList<String> keyList = new ArrayList<String>();
	public ArrayList<String> dirList = new ArrayList<String>();

	public KalyanImageToPdfWritable() {

	}

	public KalyanImageToPdfWritable(ArrayList<BufferedImage> buff, ArrayList<String> name, ArrayList<String> dir) {
		this.bufferList = buff;
		log.info("adding images " + bufferList.size());
		this.keyList = name;
		this.dirList = dir;
	}

	public BufferedImage getImage(int i) {
		return this.bufferList.get(i);
	}

	// reading generated pdf files
	@Override
	public void readFields(DataInput in) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		int newlength = WritableUtils.readVInt(in);
		bytes = new byte[newlength];
		in.readFully(bytes, 0, newlength);
		log.info("this is readFields of ImageToPdfWritable of scanned");
		try {
			DataInputBuffer ins = (DataInputBuffer) in;
			ins.reset();
			Document doc = new Document();
			PdfCopy copy = new PdfCopy(doc, b);
			reader = new PdfReader(bytes);
			doc.open();
			int inc = 0;
			while (inc < reader.getNumberOfPages()) {
				inc++;
				copy.addPage(copy.getImportedPage(reader, inc));
			}
			reader.close();
			doc.close();
			ins.close();
			log.info(ins.getLength());
		} catch (Exception e) {
			log.info(e);
		}
	}

	// writing the image files to pdf files
	@Override
	public void write(DataOutput out) throws IOException {
		log.info("beginning write in ImageToPdfWritable " + bufferList.size());
		Document document = new Document();
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ImageIO.write(bufferList.get(i), "jpeg", b);
		b.flush();
		bytes = b.toByteArray();
		b.close();
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			PdfWriter.getInstance(document, output);
			document.open();
			String keyname = keyList.get(i).toString().substring(0, keyList.get(i).toString().length() - 4);
			log.info(keyname);
			document.add(new Paragraph(keyname));
			Image image = Image.getInstance(bytes);
			image.scaleAbsolute(520, 750);
			document.add(image);
			document.close();
			WritableUtils.writeVInt(out, output.size());
			out.write(output.toByteArray(), 0, output.size());
			i++;
		} catch (Exception e) {
			log.info("error in write of ImageToPdfWritable :" + e);

		}
	}
}