package cloudflow.core.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import cloudflow.core.hadoop.records.IWritableRecord;
import cloudflow.core.records.IRecordConsumer;
import cloudflow.core.records.Record;

public class RecordToContextWriter implements IRecordConsumer<Record<?, ?>> {

	private TaskInputOutputContext context;

	private IWritableRecord writableRecord;

	private HadoopRecordKey hadoopKey = new HadoopRecordKey();

	private HadoopRecordValue hadoopValue = new HadoopRecordValue();

	public RecordToContextWriter(TaskInputOutputContext context) {
		this.context = context;

		try {
			String recordClassName = context.getConfiguration().get(
					"cloudflow.steps.map.output");
			Class<? extends Record<?, ?>> recordClass = (Class<? extends Record<?, ?>>) Class
					.forName(recordClassName);
			writableRecord = MapReduceRunner.createWritableRecord(recordClass);
			
			hadoopKey.setConf(context.getConfiguration());
			hadoopValue.setConf(context.getConfiguration());
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void consume(Record<?, ?> record) {
		try {
			WritableComparable myKey = writableRecord.fillWritableKey(record);
			Writable myValue = writableRecord.fillWritableValue(record);

			hadoopKey.set(myKey);
			hadoopValue.set(myValue);

			if (!(myValue instanceof IntWritable)){
				System.out.println("fuck!! value: " + myValue.getClass().getName());	
			}
			
			
			
			
			
			
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
