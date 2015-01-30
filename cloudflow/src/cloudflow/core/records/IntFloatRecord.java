package cloudflow.core.records;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

public class IntFloatRecord extends Record<IntWritable, FloatWritable> {

	public IntFloatRecord() {
		setWritableKey(new IntWritable());
		setWritableValue(new FloatWritable());
	}

	public float getValue() {
		return getWritableValue().get();
	}

	public void setValue(float value) {
		getWritableValue().set(value);
	}

	public int getKey() {
		return getWritableKey().get();
	}

	public void setKey(int key) {
		getWritableKey().set(key);
	}

}
