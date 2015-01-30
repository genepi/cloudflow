package cloudflow.core.operations;

import cloudflow.core.hadoop.RecordValues;
import cloudflow.core.records.IntFloatRecord;
import cloudflow.core.records.IntIntRecord;

public class Mean extends ReduceStep<IntIntRecord, IntFloatRecord> {

	private IntFloatRecord outRecord = new IntFloatRecord();

	// TODO: atm: key is converted to String. --> change!!

	public Mean() {
		super(IntIntRecord.class, IntFloatRecord.class);
	}

	@Override
	public void process(String key, RecordValues<IntIntRecord> values) {

		int sum = 0;
		int count = 0;
		while (values.hasNextRecord()) {
			sum += values.getRecord().getValue();
			count++;
		}
		outRecord.setKey(Integer.parseInt(key));
		outRecord.setValue(sum / (float) count);
		emit(outRecord);
	}

}