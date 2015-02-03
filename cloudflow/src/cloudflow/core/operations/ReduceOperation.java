package cloudflow.core.operations;

import cloudflow.core.PipelineConf;
import cloudflow.core.hadoop.RecordList;
import cloudflow.core.hadoop.GroupedRecords;
import cloudflow.core.records.Record;

public abstract class ReduceOperation<IN extends Record<?,?>, OUT extends Record<?,?>> {

	private RecordList records = new RecordList();
 
	private Class<IN> inputRecordClass;

	private Class<OUT> outputRecordClass;

	public ReduceOperation(Class<IN> inputRecordClass, Class<OUT> outputRecordClass) {
		this.inputRecordClass = inputRecordClass;
		this.outputRecordClass = outputRecordClass;
	}
	
	public abstract void process(String key, GroupedRecords<IN> values);

	public void configure(PipelineConf conf){
		
	}
	
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
