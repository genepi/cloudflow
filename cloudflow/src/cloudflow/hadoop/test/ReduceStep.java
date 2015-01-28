package cloudflow.hadoop.test;

import cloudflow.hadoop.records.Record;
import cloudflow.hadoop.records.RecordList;
import cloudflow.hadoop.records.RecordValues;

public abstract class ReduceStep {

	private RecordList records = new RecordList();

	private Record record = new Record();

	public abstract void process(String key, RecordValues values);

	public void createRecord(Record record) {
		records.add(record);
	}

	public void createRecord(String key, String value) {
		record.setKey(key);
		record.setValue(value);
		records.add(record);
	}

	public RecordList getOutputRecords() {
		return records;
	}

}
