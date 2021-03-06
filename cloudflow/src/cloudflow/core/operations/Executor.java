package cloudflow.core.operations;

import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;
import cloudflow.core.records.GroupedRecords;
import cloudflow.core.records.TextRecord;

public abstract class Executor extends Summarizer<TextRecord, TextRecord> {

	private TextRecord outRecord;

	public Executor() {
		super(TextRecord.class, TextRecord.class);
		outRecord = new TextRecord();
	}

	public abstract boolean execute(String inputFilename, String outputFilename);

	@Override
	public void summarize(String key, GroupedRecords<TextRecord> values) {
		String inputFilename = "/tmp/input-" + key + ".txt";
		String outputFilename = "/tmp/output-" + key + ".txt";
		try {
			// write records to input file
			LineWriter writer = new LineWriter(inputFilename);
			while (values.hasNextRecord()) {
				writer.write(values.getRecord().getValue());
			}
			writer.close();
			// execute user defined program
			boolean result = execute(inputFilename, outputFilename);
			// read output file and create for each line one records
			if (result) {
				LineReader reader = new LineReader(outputFilename);
				while (reader.next()) {
					outRecord.setValue(reader.get());
					emit(outRecord);
				}
				reader.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
