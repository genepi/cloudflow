package cloudflow.core.operations;

import cloudflow.core.hadoop.GroupedRecords;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.records.TextRecord;

public class Concat extends ReduceOperation<TextRecord, TextRecord> {

	private TextRecord outRecord = new TextRecord();

	public Concat() {
		super(TextRecord.class, TextRecord.class);
	}

	@Override
	public void process(String key, GroupedRecords<TextRecord> values) {

		StringBuilder builder = new StringBuilder();
		builder.append("#VCF File for chromosome "+key);
		while (values.hasNextRecord()) {
			outRecord.setKey(key);
			outRecord.setValue(values.getRecord().getValue());
			emit(outRecord);
		}
		
	}

}