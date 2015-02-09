package cloudflow.core.records;

import org.apache.hadoop.io.Text;

import cloudflow.bio.fastq.SingleRead;

public class ShortReadRecord extends Record<Text, SingleRead> {

	public ShortReadRecord() {
		setWritableKey(new Text());
		setWritableValue(new SingleRead());
	}



}
