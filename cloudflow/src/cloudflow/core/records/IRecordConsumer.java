package cloudflow.core.records;



public interface IRecordConsumer<IN extends Record> {

	public void consume(IN record);
	
}
