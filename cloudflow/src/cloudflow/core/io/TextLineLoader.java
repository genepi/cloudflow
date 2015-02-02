package cloudflow.core.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import cloudflow.core.records.TextRecord;

public class TextLineLoader implements ILoader {

	private int lines = 100;

	public TextLineLoader(){
		
	}
	
	public TextLineLoader(int lines){
		this.lines = lines;
	}
	
	@Override
	public Class getInputFormat() {
		return NLineInputFormat.class;
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
		conf.setInt("mapreduce.input.lineinputformat.linespermap", lines);
	}

	public void setNumberOfLine(int lines) {
		this.lines = lines;
	}

}