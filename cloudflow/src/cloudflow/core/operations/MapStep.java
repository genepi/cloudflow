package cloudflow.core.operations;

import cloudflow.core.PipelineConf;
import cloudflow.core.hadoop.IRecordConsumer;
import cloudflow.core.hadoop.RecordList;
import cloudflow.core.records.Record;

public abstract class MapStep<IN extends Record<?, ?>, OUT extends Record<?, ?>>
		implements IRecordConsumer<IN> {

	private RecordList records = new RecordList();

	private Class<IN> inputRecordClass;

	private Class<OUT> outputRecordClass;

	public MapStep(Class<IN> inputRecordClass, Class<OUT> outputRecordClass) {
		this.inputRecordClass = inputRecordClass;
		this.outputRecordClass = outputRecordClass;
	}

	public void configure(PipelineConf conf){
		
	}
	
	public abstract void process(IN record);

	public void emit(OUT record) {
		records.add(record);
	}

	@Override
	public void consume(IN record) {
		process(record);
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
