package cloudflow.core.records;

public interface IRecordProducer {

	public abstract void addConsumer(IRecordConsumer consumer);
	
}
