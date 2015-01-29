package cloudflow.core.hadoop;

import cloudflow.core.records.Record;

public class RecordValues<IN extends Record<?, ?>> {

	protected Iterable<HadoopRecordValue> values;

	private IN record;

	public RecordValues(Iterable<HadoopRecordValue> values) {
		this.values = values;
	}

	public RecordValues() {

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
