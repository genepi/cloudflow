package cloudflow.core.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import cloudflow.core.records.IRecordConsumer;
import cloudflow.core.records.Record;

public class RecordToContextWriter2 implements IRecordConsumer<Record<?, ?>> {

	private TaskInputOutputContext context;

	private Text hadoopKey = new Text();

	private Text hadoopValue = new Text();

	public RecordToContextWriter2(TaskInputOutputContext context) {
		this.context = context;
	}

	@Override
	public void consume(Record<?, ?> record) {
		try {

			hadoopKey.set(record.getKey().toString());
			hadoopValue.set(record.getValue().toString());
			context.write(hadoopKey, hadoopValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
