package cloudflow.hadoop.test;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cloudflow.hadoop.records.RecordToContextWriter;
import cloudflow.hadoop.records.RecordValues;

public class GenericReducer extends Reducer<Text, Text, Text, Text> {

	private SerializableSteps<ReduceStep> reduceSteps;

	private SerializableSteps<MapStep> filterSteps;

	private ReduceStep reduceStep;

	private RecordValues recordValues = new RecordValues();

	private List<MapStep> instancesFilter = new Vector<MapStep>();

	@Override
	protected void setup(final Context context) throws IOException,
			InterruptedException {

		try {

			// read reduce step
			String data = context.getConfiguration().get(
					"cloudflow.steps.reduce");
			reduceSteps = new SerializableSteps<ReduceStep>();
			reduceSteps.load(data);

			List<ReduceStep> instancesReduce = reduceSteps.createInstances();
			reduceStep = instancesReduce.get(0);

			// read filter steps
			String dataMap = context.getConfiguration().get(
					"cloudflow.steps.map2");

			filterSteps = new SerializableSteps<MapStep>();
			if (dataMap != null) {
				filterSteps.load(dataMap);
			}

			if (filterSteps.getSize() > 0) {

				instancesFilter = filterSteps.createInstances();

				// fist step consumes reduce step output records
				reduceStep.getOutputRecords().addConsumer(
						instancesFilter.get(0));

				// step n + 1 consumes records produced by n
				for (int i = 0; i < instancesFilter.size() - 1; i++) {
					MapStep step = instancesFilter.get(i);
					MapStep nextStep = instancesFilter.get(i + 1);
					step.getOutputRecords().addConsumer(nextStep);
				}

				// last step writes records to context
				instancesFilter.get(instancesFilter.size() - 1)
						.getOutputRecords()
						.addConsumer(new RecordToContextWriter(context));
			} else {

				// reduce step writes records to context
				reduceStep.getOutputRecords().addConsumer(
						new RecordToContextWriter(context));

			}

		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new IOException(e);
		}

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		recordValues.setValues(values);
		reduceStep.process(key.toString(), recordValues);

	}

}
