package cloudflow.core.hadoop;

import cloudflow.core.records.Record;


public interface IRecordConsumer<IN extends Record> {

	public void consume(IN record);
	
}
