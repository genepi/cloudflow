package cloudflow.hadoop.test;

import org.apache.hadoop.io.Text;

public class Record {

	private String key;

	private String value;

	public Record(String key, String value){
		this.key = key;
		this.value = value;
	}
	
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

}
