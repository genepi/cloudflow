package cloudflow.core.hadoop;

import cloudflow.core.records.Record;

public class GroupedRecords<IN extends Record<?, ?>> {

	protected Iterable<HadoopRecordValue> values;

	private IN record;

	public GroupedRecords(Iterable<HadoopRecordValue> values) {
		this.values = values;
	}

	public GroupedRecords() {

	}

	public void setRecordClassName(Class<?> recordClassName)
			throws InstantiationException, IllegalAccessException {
		record = (IN) recordClassName.newInstance();
	}

	public void setValues(Iterable<HadoopRecordValue> values) {
		this.values = values;
	}

	public boolean hasNextRecord() {
		return values.iterator().hasNext();
	}

	public IN getRecord() {
		HadoopRecordValue hadoopRecord = values.iterator().next();
		record.setWritableValue(hadoopRecord.get());
		return (IN) record;
	}

}
