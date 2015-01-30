package cloudflow.bio.fastq;

import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.SequencedFragment;

import cloudflow.core.records.Record;

public class FastqRecord extends Record<Text, SequencedFragment> {

	public FastqRecord() {
		setWritableKey(new Text());
		setWritableValue(new SequencedFragment());
	}

	public SequencedFragment getValue() {
		return getWritableValue();
	}

	public void setValue(SequencedFragment value) {
		getWritableValue();
	}

	public String getKey() {
		return getWritableKey().toString();
	}

	public void setKey(String key) {
		getWritableKey().set(key);
	}
}
