package cloudflow.bio.bam;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import cloudflow.core.hadoop.records.IWritableRecord;
import cloudflow.core.records.Record;

public class BamWritableRecord implements IWritableRecord {

	private Text key = new Text();

	private SAMRecordWritable value = new SAMRecordWritable();

	private BamRecord record = new BamRecord();

	@Override
	public WritableComparable fillWritableKey(Record record) {
		BamRecord floatRecord = (BamRecord) record;
		key.set(floatRecord.getKey());
		return key;
	}

	@Override
	public Writable fillWritableValue(Record record) {
		BamRecord floatRecord = (BamRecord) record;
		value.set(floatRecord.getValue());
		return value;
	}

	@Override
	public Record fillRecord(WritableComparable key, Writable value) {
		record.setKey(key.toString());
		record.setValue(((SAMRecordWritable)value).get());
		return record;
	}

}
