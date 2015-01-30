package cloudflow.core.operations;

import cloudflow.core.hadoop.RecordList;
import cloudflow.core.hadoop.RecordValues;
import cloudflow.core.records.Record;

public abstract class ReduceStep<IN extends Record<?,?>, OUT extends Record<?,?>> {

	private RecordList records = new RecordList();
 
	public abstract void process(String key, RecordValues<IN> values);

	public void emit(OUT record) {
		records.add(record);
	}

	public RecordList getOutputRecords() {
		return records;
	}

}
