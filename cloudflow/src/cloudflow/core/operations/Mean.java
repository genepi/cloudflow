package cloudflow.core.operations;

import cloudflow.core.hadoop.GroupedRecords;
import cloudflow.core.records.FloatRecord;
import cloudflow.core.records.IntegerRecord;

public class Mean extends ReduceOperation<IntegerRecord, FloatRecord> {

	private FloatRecord outRecord = new FloatRecord();

	// TODO: atm: key is converted to String. --> change!!

	public Mean() {
		super(IntegerRecord.class, FloatRecord.class);
	}

	@Override
	public void process(String key, GroupedRecords<IntegerRecord> values) {

		int sum = 0;
		int count = 0;
		while (values.hasNextRecord()) {
			sum += values.getRecord().getValue();
			count++;
		}
		outRecord.setKey(key);
		outRecord.setValue(sum / (float) count);
		emit(outRecord);
	}

}