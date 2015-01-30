package cloudflow.core.operations;

import cloudflow.core.hadoop.RecordList;
import cloudflow.core.hadoop.RecordValues;
import cloudflow.core.records.Record;

public abstract class ReduceStep<IN extends Record<?,?>, OUT extends Record<?,?>> {

	private RecordList records = new RecordList();
 
	private Class<IN> inputRecordClass;

	private Class<OUT> outputRecordClass;

	public ReduceStep(Class<IN> inputRecordClass, Class<OUT> outputRecordClass) {
		this.inputRecordClass = inputRecordClass;
		this.outputRecordClass = outputRecordClass;
	}
	
	public abstract void process(String key, RecordValues<IN> values);

	public void emit(OUT record) {
		records.add(record);
	}

	public RecordList getOutputRecords() {
		return records;
	}
	

	public Class<OUT> getOutputRecordClass() {
		return outputRecordClass;
	}

	public Class<IN> getInputRecordClass() {
		return inputRecordClass;
	}

}
