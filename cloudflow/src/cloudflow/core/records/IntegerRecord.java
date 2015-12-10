package cloudflow.core.records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class IntegerRecord extends Record<Text, IntWritable> {

	private String key;

	private int value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return getKey().toString() +"\t" + getValue();
	}
	
}
