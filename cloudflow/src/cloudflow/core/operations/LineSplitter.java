package cloudflow.core.operations;

import cloudflow.core.records.TextRecord;

public class LineSplitter extends MapOperation<TextRecord, TextRecord> {

	private TextRecord outRecord = new TextRecord();
	
	private String key = "";

	public LineSplitter() {
		super(TextRecord.class, TextRecord.class);
		key = System.currentTimeMillis()+"";
	}

	@Override
	public void process(TextRecord record) {
		outRecord.setKey(key);
		outRecord.setValue(record.getValue());
		emit(outRecord);
	}

}
