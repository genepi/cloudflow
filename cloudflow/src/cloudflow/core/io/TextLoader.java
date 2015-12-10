package cloudflow.core.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import cloudflow.core.hadoop.HadoopRecordFileLoader;
import cloudflow.core.local.TextFileRecordReader;
import cloudflow.core.records.TextRecord;

public class TextLoader implements HadoopRecordFileLoader, LocalFileLoader {

	@Override
	public Class getInputFormat() {

		return TextInputFormat.class;
	}

	@Override
	public Class<?> getInputKeyClass() {
		return Object.class;
	}

	@Override
	public Class<? extends Writable> getInputValueClass() {
		return Text.class;
	}

	@Override
	public Class<?> getRecordClass() {
		return TextRecord.class;
	}

	@Override
	public void configure(Configuration conf) {

	}

	@Override
	public FileRecordReader<?> createFileRecordReader(String filename) {
		TextFileRecordReader reader = new TextFileRecordReader();
		reader.open(filename);
		return reader;
	}

}