package cloudflow.hadoop.test;

import cloudflow.hadoop.records.Record;

public abstract class RecordFilter extends MapStep {

	@Override
	public void process(Record record) {
		if (!filter(record)) {
			createRecord(record);
		}
	}

	public abstract boolean filter(Record record);

}