package cloudflow.bio.bam;

import htsjdk.samtools.SAMRecord;
import cloudflow.core.records.Record;

public class BamRecord extends Record<String, SAMRecord> {

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
