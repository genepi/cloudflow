package cloudflow.core.spark;

import java.util.Iterator;

import cloudflow.core.records.GroupedRecords;
import cloudflow.core.records.Record;

public class SparkGroupedRecords<IN extends Record<?, ?>> implements GroupedRecords<IN>{

	protected Iterator<Record> values;

	private Object key;


	public SparkGroupedRecords() {

	}
	
	public void setValues(Iterator<Record> values) {
		this.values = values;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public boolean hasNextRecord() {
		return values.hasNext();
	}

	public IN getRecord() {
		Record record = (Record) values.next();
		record.setKey(key);
		return (IN) record;
	}

}
