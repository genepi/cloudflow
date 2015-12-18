package cloudflow.bio.fastq;

import genepi.io.text.LineReader;

import java.io.IOException;

import cloudflow.core.io.FileRecordReader;

public class FastqFileRecordReader extends FileRecordReader<FastqRecord> {

	private LineReader reader;

	private FastqRecord record = new FastqRecord();

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
	public FastqRecord next() {
				
		try {
			if (reader.next()) {
				//line1
				String id = reader.get();
				record.setKey(reader.get());

				//line2
				reader.next();
				String sequence = reader.get();
				
				//line3
				reader.next();
				reader.get();
				
				//line4
				reader.next();
				String quality = reader.get();
				
				record.getValue().getSequence().set(sequence);
				record.getValue().getQuality().set(quality);

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
