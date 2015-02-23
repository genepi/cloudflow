package cloudflow.core.operations;

import cloudflow.core.records.TextRecord;

public class LineSplitter extends Transformer<TextRecord, TextRecord> {

	private TextRecord outRecord = new TextRecord();
	
	private String key = "";

	public LineSplitter() {
		super(TextRecord.class, TextRecord.class);
		key = System.currentTimeMillis()+"";
	}

	@Override
	public void transform(TextRecord record) {
		outRecord.setKey(key);
		outRecord.setValue(record.getValue());
		emit(outRecord);
	}

}
