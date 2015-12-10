package cloudflow.core.local;

import genepi.io.text.LineWriter;

import java.io.IOException;

import cloudflow.core.records.IRecordConsumer;
import cloudflow.core.records.Record;

public class RecordToFileWriter implements IRecordConsumer<Record<?, ?>> {

	private LineWriter writer;
	
	public RecordToFileWriter(String filename){
		try {
			writer = new LineWriter(filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void consume(Record<?, ?> record) {
		try {
			writer.write(record.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void close() {
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
