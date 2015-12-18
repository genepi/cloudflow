package cloudflow.core.hadoop.records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import cloudflow.core.records.IntegerRecord;
import cloudflow.core.records.Record;

public class IntegerWritableRecord implements IWritableRecord {

	private Text key = new Text();

	private IntWritable value = new IntWritable();

	private IntegerRecord record = new IntegerRecord();

		
	@Override
	public WritableComparable fillWritableKey(Record record) {
		IntegerRecord floatRecord = (IntegerRecord) record;
		key.set(floatRecord.getKey());
		return key;
	}

	@Override
	public Writable fillWritableValue(Record record) {
		IntegerRecord floatRecord = (IntegerRecord) record;
		value.set(floatRecord.getValue());
		return value;
	}

	@Override
	public Record fillRecord(WritableComparable key, Writable value) {
		record.setKey(key.toString());
		record.setValue(((IntWritable) value).get());
		return record;
	}

}
