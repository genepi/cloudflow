package cloudflow.core.records;

import genepi.hadoop.io.HdfsLineWriter;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RecordWriter<T extends Record<? extends Writable, ? extends Writable>> {

	private HdfsLineWriter writer;

	public RecordWriter(String filename) {	
		try {
			writer = new HdfsLineWriter(filename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void write(T record) {
		try {
			writer.write(record.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
