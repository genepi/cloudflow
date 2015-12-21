package cloudflow.core.spark;

import java.io.Serializable;
import java.util.Iterator;

import cloudflow.core.records.GroupedRecords;
import cloudflow.core.records.Record;

public class SparkGroupedRecords<IN extends Record<?, ?>> implements GroupedRecords<IN>, Serializable{

	protected Iterator<Object> values;

	private Object key;

	private Record record;
	
	public SparkGroupedRecords(Class inputRecordClass) throws InstantiationException, IllegalAccessException {
		record = (Record) inputRecordClass.newInstance();
	}
	
	public void setValues(Iterator<Object> values) {
		this.values = values;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public boolean hasNextRecord() {
		return values.hasNext();
	}

	public IN getRecord() {
		Object value = values.next();
		record.setKey(key);
		record.setValue(value);
		return (IN) record;
	}

}
