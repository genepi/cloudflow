package cloudflow.bio.fastq;

import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.SequencedFragment;

import cloudflow.core.records.Record;

public class FastqRecord extends Record<Text, SequencedFragment> {

	private String key;

	private SequencedFragment value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public SequencedFragment getValue() {
		return value;
	}

	public void setValue(SequencedFragment value) {
		this.value = value;
	}

}
