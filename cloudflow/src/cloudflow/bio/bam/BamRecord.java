package cloudflow.bio.bam;

import htsjdk.samtools.SAMRecord;

import org.apache.hadoop.io.IntWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import cloudflow.core.records.Record;

public class BamRecord extends Record<IntWritable, SAMRecordWritable> {

	private String key;

	private SAMRecord value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public SAMRecord getValue() {
		return value;
	}

	public void setValue(SAMRecord value) {
		this.value = value;
	}

}
