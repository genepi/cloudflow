package cloudflow.core.operations;

import cloudflow.core.hadoop.RecordValues;
import cloudflow.core.records.IntegerRecord;

public class Sum extends ReduceStep<IntegerRecord, IntegerRecord> {

	private IntegerRecord outRecord = new IntegerRecord();

	// TODO: atm: key is converted to String. --> change!!

	public Sum() {
		super(IntegerRecord.class, IntegerRecord.class);
	}

	@Override
	public void process(String key, RecordValues<IntegerRecord> values) {

		int sum = 0;
		while (values.hasNextRecord()) {
			sum += values.getRecord().getValue();
		}
		outRecord.setKey(key);
		outRecord.setValue(sum);
		emit(outRecord);
	}

}