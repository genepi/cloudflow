package cloudflow.core.records;

import org.apache.hadoop.io.IntWritable;

public class IntIntRecord extends Record<IntWritable, IntWritable> {

	public IntIntRecord() {
		setWritableKey(new IntWritable());
		setWritableValue(new IntWritable());
	}

	public int getValue() {
		return getWritableValue().get();
	}

	public void setValue(int value) {
		getWritableValue().set(value);
	}

	public int getKey() {
		return getWritableKey().get();
	}

	public void setKey(int key) {
		getWritableKey().set(key);
	}

}
