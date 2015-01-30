package cloudflow.core.records;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class StringFloatRecord extends Record<Text, FloatWritable> {

	public StringFloatRecord() {
		setWritableKey(new Text());
		setWritableValue(new FloatWritable());
	}

	public float getValue() {
		return getWritableValue().get();
	}

	public void setValue(float value) {
		getWritableValue().set(value);
	}

	public String getKey() {
		return getWritableKey().toString();
	}

	public void setKey(String key) {
		getWritableKey().set(key);
	}

}
