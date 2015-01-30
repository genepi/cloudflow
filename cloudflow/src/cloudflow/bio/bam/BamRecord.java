package cloudflow.bio.bam;

import htsjdk.samtools.SAMRecord;

import org.apache.hadoop.io.IntWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import cloudflow.core.records.Record;

public class BamRecord extends Record<IntWritable, SAMRecordWritable> {

	public BamRecord() {
		setWritableKey(new IntWritable());
		setWritableValue(new SAMRecordWritable());
	}

	public SAMRecord getValue() {
		return getWritableValue().get();
	}

	public void setValue(SAMRecord value) {
		getWritableValue().set(value);
	}

	public int getKey() {
		return getWritableKey().get();
	}

	public void setKey(int key) {
		getWritableKey().set(key);
	}
}
