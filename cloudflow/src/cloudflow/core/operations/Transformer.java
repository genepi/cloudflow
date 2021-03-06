package cloudflow.core.operations;

import cloudflow.core.PipelineConf;
import cloudflow.core.records.IRecordConsumer;
import cloudflow.core.records.Record;
import cloudflow.core.records.RecordList;

public abstract class Transformer<IN extends Record<?, ?>, OUT extends Record<?, ?>>
		implements IRecordConsumer<IN>, IOperation {

	private RecordList records = new RecordList();

	private Class<IN> inputRecordClass;

	private Class<OUT> outputRecordClass;

	public Transformer(Class<IN> inputRecordClass, Class<OUT> outputRecordClass) {
		this.inputRecordClass = inputRecordClass;
		this.outputRecordClass = outputRecordClass;
	}

	public void configure(PipelineConf conf){
		
	}
	
	public abstract void transform(IN record);

	public void emit(OUT record) {
		records.add(record);
	}

	@Override
	public void consume(IN record) {
		transform(record);
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
