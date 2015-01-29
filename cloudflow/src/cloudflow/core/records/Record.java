package cloudflow.core.records;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class Record<KEY extends WritableComparable<?>, VALUE extends Writable> {

	private KEY key;

	private VALUE value;

	public Record() {

	}

	public KEY getWritableKey() {
		return key;
	}

	public void setWritableKey(WritableComparable<?> key) {
		this.key = (KEY) key;
	}

	public VALUE getWritableValue() {
		return value;
	}

	public void setWritableValue(Writable value) {
		this.value = (VALUE) value;
	}

	public Class<?> getWritableKeyClass() {
		return key.getClass();
	}

	public Class<?> getWritableValueClass() {
		return value.getClass();
	}

}
