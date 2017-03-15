package com.orienit.kalyan.hadoop.training.ngram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

public class NGramKey implements WritableComparable<NGramKey> {
	String separator = "::";

	List<String> words;

	@Override
	public String toString() {
		return StringUtils.join(words, " ");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((words == null) ? 0 : words.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NGramKey other = (NGramKey) obj;
		if (words == null) {
			if (other.words != null)
				return false;
		} else if (!words.equals(other.words))
			return false;
		return true;
	}

	public List<String> getWords() {
		return words;
	}

	public void setWords(List<String> words) {
		this.words = words;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String data = in.readUTF();
		String[] splits = StringUtils.split(data, separator);
		this.words = Arrays.asList(splits);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		String join = StringUtils.join(this.words, separator);
		out.writeUTF(join);
	}

	@Override
	public int compareTo(NGramKey key) {
		String key1 = StringUtils.join(this.words, separator);
		String key2 = StringUtils.join(key.words, separator);
		return key1.compareTo(key2);
	}

}
