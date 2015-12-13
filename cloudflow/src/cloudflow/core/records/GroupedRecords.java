package cloudflow.core.records;

import java.util.Iterator;

import cloudflow.core.hadoop.records.IWritableRecord;
import cloudflow.core.records.Record;

public interface GroupedRecords<IN extends Record<?, ?>> {

	public boolean hasNextRecord();

	public IN getRecord();

}
