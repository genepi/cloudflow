package cloudflow.core.records;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class FloatRecord extends Record<Text, FloatWritable> {

	private String key;

	private float value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return getKey().toString() +"\t" + getValue();
	}

}
