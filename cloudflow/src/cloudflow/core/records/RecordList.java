package cloudflow.core.records;

import java.util.List;
import java.util.Vector;

public class RecordList implements IRecordProducer{

	private List<IRecordConsumer> consumers = new Vector<IRecordConsumer>();

	public void add(Record record) {
		notifyConsumers(record);
	}

	private void notifyConsumers(Record record) {
		for (IRecordConsumer consumer : consumers) {
			consumer.consume(record);
		}
	}

	public void addConsumer(IRecordConsumer consumer) {
		consumers.add(consumer);
	}

}
