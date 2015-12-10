package cloudflow.core.hadoop.records;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.records.FloatRecord;
import cloudflow.core.records.Record;

public class FloatWritableRecord implements IWritableRecord {

	private Text key = new Text();

	private FloatWritable value = new FloatWritable();

	private FloatRecord record = new FloatRecord();

	@Override
	public WritableComparable fillWritableKey(Record record) {
		FloatRecord floatRecord = (FloatRecord) record;
		key.set(floatRecord.getKey());
		return key;
	}

	@Override
	public Writable fillWritableValue(Record record) {
		FloatRecord floatRecord = (FloatRecord) record;
		value.set(floatRecord.getValue());
		return value;
	}

	@Override
	public Record fillRecord(WritableComparable key, Writable value) {
		record.setKey(key.toString());
		record.setValue(((FloatWritable)value).get());
		return record;
	}

}
