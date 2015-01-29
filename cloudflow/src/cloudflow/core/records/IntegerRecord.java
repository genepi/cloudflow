package cloudflow.core.records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class IntegerRecord extends Record<Text, IntWritable> {

	public IntegerRecord() {
		setWritableKey(new Text());
		setWritableValue(new IntWritable());
	}

	public int getValue() {
		return getWritableValue().get();
	}

	public void setValue(int value) {
		getWritableValue().set(value);
	}

	public String getKey() {
		return getWritableKey().toString();
	}

	public void setKey(String key) {
		getWritableKey().set(key);
	}

}
