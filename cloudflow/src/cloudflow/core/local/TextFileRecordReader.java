package cloudflow.core.local;

import genepi.io.text.LineReader;

import java.io.IOException;

import cloudflow.core.io.FileRecordReader;
import cloudflow.core.records.TextRecord;

public class TextFileRecordReader extends FileRecordReader<TextRecord> {

	private LineReader reader;

	private TextRecord record = new TextRecord();

	@Override
	public boolean open(String filename) {
		try {
			reader = new LineReader(filename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public TextRecord next() {
		try {
			if (reader.next()) {
				record.setKey("1");
				record.setValue(reader.get());
				return record;
			} else {
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void close() {
		reader.close();

	}

}
