package cloudflow.core.operations;

import cloudflow.core.hadoop.IRecordConsumer;
import cloudflow.core.hadoop.RecordList;
import cloudflow.core.records.Record;

public abstract class MapStep<IN extends Record<?, ?>, OUT extends Record<?, ?>>
		implements IRecordConsumer<IN> {

	private RecordList records = new RecordList();

	public abstract void process(IN record);

	public void emit(OUT record) {
		records.add(record);
	}

	@Override
	public void consume(IN record) {
		process(record);
	}

	public RecordList getOutputRecords() {
		return records;
	}

}
