package cloudflow.hadoop.test;

import cloudflow.hadoop.records.IRecordConsumer;
import cloudflow.hadoop.records.Record;
import cloudflow.hadoop.records.RecordList;

public abstract class MapStep implements IRecordConsumer {

	private RecordList records = new RecordList();

	private Record record = new Record();

	public abstract void process(Record record);

	public void createRecord(Record record) {
		records.add(record);
	}

	public void createRecord(String key, String value) {
		record.setKey(key);
		record.setValue(value);
		records.add(record);
	}

	@Override
	public void consume(Record record) {
		process(record);
	}

	public RecordList getOutputRecords() {
		return records;
	}

}
