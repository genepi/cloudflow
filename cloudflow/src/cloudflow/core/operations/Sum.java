package cloudflow.core.operations;

import cloudflow.core.hadoop.GroupedRecords;
import cloudflow.core.records.IntegerRecord;

public class Sum extends Summarizer<IntegerRecord, IntegerRecord> {

	private IntegerRecord outRecord = new IntegerRecord();

	public Sum() {
		super(IntegerRecord.class, IntegerRecord.class);
	}

	@Override
	public void summarize(String key, GroupedRecords<IntegerRecord> values) {

		int sum = 0;
		while (values.hasNextRecord()) {
			sum += values.getRecord().getValue();
		}
		outRecord.setKey(key);
		outRecord.setValue(sum);
		emit(outRecord);
	}

}