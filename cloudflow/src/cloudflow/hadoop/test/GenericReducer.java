package cloudflow.hadoop.test;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenericReducer extends Reducer<Text, Text, Text, Text> {

	private SerializableSteps<IReduceStep> steps;

	private Text newKey = new Text();

	private Text newValue = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		// read reduce steps
		String data = context.getConfiguration().get("cloudflow.steps.reduce");
		try {
			steps = new SerializableSteps<IReduceStep>();
			steps.load(data);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<Record> records = new Vector<>();
		for (Text value : values) {
			records.add(new Record(key.toString(), value.toString()));
		}

		try {
			// execute steps
			for (int i = 0; i < steps.getSize(); i++) {
				IReduceStep step = steps.getStepInstance(i);
				records = step.process(records);
			}
		} catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(e);
		}

		for (Record record : records) {
			newKey.set(record.getKey());
			newValue.set(record.getValue());
			context.write(newKey, newValue);
		}

	}

}
