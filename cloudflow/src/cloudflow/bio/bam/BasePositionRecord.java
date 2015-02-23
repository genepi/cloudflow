package cloudflow.bio.bam;

import org.apache.hadoop.io.IntWritable;

import cloudflow.core.records.Record;

public class BasePositionRecord extends Record<IntWritable, BasePosition> {

	public BasePositionRecord() {
		setWritableKey(new IntWritable());
		setWritableValue(new BasePosition());
	}

	public BasePosition getValue() {
		return getWritableValue();
	}

	public void setValue(BasePosition value) {
		setWritableValue(value);
	}

	public int getKey() {
		return getWritableKey().get();
	}

	public void setKey(int key) {
		getWritableKey().set(key);
	}
}
