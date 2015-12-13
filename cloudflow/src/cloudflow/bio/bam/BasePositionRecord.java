package cloudflow.bio.bam;

import org.apache.hadoop.io.IntWritable;

import cloudflow.core.records.Record;

public class BasePositionRecord extends Record<Integer, BasePosition> {

	private int key;

	private BasePosition value;

	public Integer getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public BasePosition getValue() {
		return value;
	}

	public void setValue(BasePosition value) {
		this.value = value;
	}

}
