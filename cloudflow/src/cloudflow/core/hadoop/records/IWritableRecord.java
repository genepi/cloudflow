package cloudflow.core.hadoop.records;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import cloudflow.core.records.Record;

public interface IWritableRecord {
	public abstract WritableComparable fillWritableKey(Record record);
	
	public abstract Writable fillWritableValue(Record record);
	
	public abstract Record fillRecord(WritableComparable key, Writable value);
}
