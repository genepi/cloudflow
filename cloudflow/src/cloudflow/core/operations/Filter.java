package cloudflow.core.operations;

import cloudflow.core.records.Record;

public abstract class Filter<IN extends Record<?, ?>> extends Transformer<IN, IN> {

	public Filter(Class<IN> recordClass) {
		super(recordClass, recordClass);
	}

	@Override
	public void transform(IN record) {
		if (!filter(record)) {
			emit(record);
		}
	}

	public abstract boolean filter(IN record);

}