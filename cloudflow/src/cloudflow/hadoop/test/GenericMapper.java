package cloudflow.hadoop.test;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Text;

import cloudflow.hadoop.records.Record;
import cloudflow.hadoop.records.RecordList;
import cloudflow.hadoop.records.RecordToContextWriter;

public class GenericMapper extends
		org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {

	private SerializableSteps<MapStep> steps;

	private List<MapStep> instances = new Vector<>();

	private RecordList inputRecords = new RecordList();

	private Record record = new Record();

	@Override
	protected void setup(final Context context) throws IOException,
			InterruptedException {

		// read mapper steps
		String data = context.getConfiguration().get("cloudflow.steps.map");
		try {
			steps = new SerializableSteps<MapStep>();
			steps.load(data);

			instances = steps.createInstances();

			// fist step consumes input records
			inputRecords.addConsumer(instances.get(0));

			// step n + 1 consumes records produced by n
			for (int i = 0; i < instances.size() - 1; i++) {
				MapStep step = instances.get(i);
				MapStep nextStep = instances.get(i + 1);
				step.getOutputRecords().addConsumer(nextStep);
			}

			// last step writes records to context
			instances.get(instances.size() - 1).getOutputRecords()
					.addConsumer(new RecordToContextWriter(context));

		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		record.setKey(key.toString());
		record.setValue(value.toString());

		inputRecords.add(record);

	}

}
