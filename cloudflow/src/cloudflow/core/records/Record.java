package cloudflow.core.records;

import java.io.Serializable;


public class Record<K extends Object, V extends Object> implements Serializable{

	private K key;

	private V value;

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return getKey().toString() +"\t" + getValue();
	}
	
}
