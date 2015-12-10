package cloudflow.core.records;

import org.apache.hadoop.io.Text;

import cloudflow.bio.fastq.SingleRead;

public class ShortReadRecord extends Record<Text, SingleRead> {

	private String key;

	private SingleRead value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public SingleRead getValue() {
		return value;
	}

	public void setValue(SingleRead value) {
		this.value = value;
	}
	
}
