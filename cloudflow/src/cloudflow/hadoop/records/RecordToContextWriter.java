package cloudflow.hadoop.records;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class RecordToContextWriter implements IRecordConsumer {

	private Text newKey = new Text();

	private Text newValue = new Text();

	private TaskInputOutputContext context;

	public RecordToContextWriter(TaskInputOutputContext context) {
		this.context = context;
	}

	@Override
	public void consume(Record record) {

		newKey.set(record.getKey());
		newValue.set(record.getValue());
		try {
			context.write(newKey, newValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
