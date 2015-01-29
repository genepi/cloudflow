package cloudflow.core.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import cloudflow.core.records.Record;

public class RecordToContextWriter implements IRecordConsumer<Record<?, ?>> {

	private HadoopRecordValue value = new HadoopRecordValue();

	private HadoopRecordKey key = new HadoopRecordKey();
	
	private TaskInputOutputContext context;

	public RecordToContextWriter(TaskInputOutputContext context) {
		this.context = context;
	}

	@Override
	public void consume(Record<?, ?> record) {

		try {
			value.set(record.getWritableValue());
			key.set(record.getWritableKey());
			context.write(key, value);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
