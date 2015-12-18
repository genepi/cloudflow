package cloudflow.core.hadoop.records;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import cloudflow.core.records.Record;
import cloudflow.core.records.TextRecord;

public class TextWritableRecord implements IWritableRecord {

	private Text key = new Text();

	private Text value = new Text();

	private TextRecord record = new TextRecord();
	
	@Override
	public WritableComparable fillWritableKey(Record record) {
		TextRecord floatRecord = (TextRecord) record;
		key.set(floatRecord.getKey());
		return key;
	}

	@Override
	public Writable fillWritableValue(Record record) {
		TextRecord floatRecord = (TextRecord) record;
		value.set(floatRecord.getValue());
		return value;
	}

	@Override
	public Record fillRecord(WritableComparable key, Writable value) {
		record.setKey(key.toString());
		record.setValue(value.toString());
		return record;
	}

}
