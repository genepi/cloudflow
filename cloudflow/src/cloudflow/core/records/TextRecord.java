package cloudflow.core.records;

import org.apache.hadoop.io.Text;

public class TextRecord extends Record<Text, Text> {

	public TextRecord() {
		setWritableKey(new Text());
		setWritableValue(new Text());
	}

	public String getValue() {
		return getWritableValue().toString();
	}

	public void setValue(String value) {
		getWritableValue().set(value);
	}

	public String getKey() {
		return getWritableKey().toString();
	}

	public void setKey(String key) {
		getWritableKey().set(key);
	}

}
