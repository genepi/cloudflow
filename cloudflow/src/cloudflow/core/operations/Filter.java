package cloudflow.core.operations;

import cloudflow.core.records.Record;

public abstract class Filter<IN extends Record<?, ?>> extends MapStep<IN, IN> {

	@Override
	public void process(IN record) {
		if (!filter(record)) {
			createRecord(record);
		}
	}

	public abstract boolean filter(IN record);

}