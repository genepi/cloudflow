package cloudflow.core.records;

import org.apache.hadoop.io.Text;

public class TextRecord extends Record<Text, Text> {

	private String key;

	private String value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return getKey().toString() +"\t" + getValue();
	}

}
