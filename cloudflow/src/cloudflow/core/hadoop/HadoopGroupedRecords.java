package cloudflow.core.hadoop;

import java.util.Iterator;

import cloudflow.core.hadoop.records.IWritableRecord;
import cloudflow.core.records.GroupedRecords;
import cloudflow.core.records.Record;

public class HadoopGroupedRecords<IN extends Record<?, ?>> implements GroupedRecords<IN>{

	protected Iterator<HadoopRecordValue> values;

	private HadoopRecordKey key;

	private IWritableRecord writableRecord;

	public HadoopGroupedRecords() {

	}

	public void setRecordClassName(Class<? extends Record<?, ?>> recordClass)
			throws InstantiationException, IllegalAccessException {
		writableRecord = MapReduceRunner.createWritableRecord(recordClass);
	}

	public void setValues(Iterator<HadoopRecordValue> values) {
		this.values = values;
	}

	public void setKey(HadoopRecordKey key) {
		this.key = key;
	}

	public boolean hasNextRecord() {
		return values.hasNext();
	}

	public IN getRecord() {
		HadoopRecordValue hadoopRecord = values.next();
		IN record = (IN) writableRecord.fillRecord(key, hadoopRecord.get());
		return record;
	}

}
