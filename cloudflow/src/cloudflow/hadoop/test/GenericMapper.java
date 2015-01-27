package cloudflow.hadoop.test;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Text;

public class GenericMapper extends
		org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {

	private SerializableSteps<IMapStep> steps;

	private Text newKey = new Text();
	private Text newValue = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		// read mapper steps
		String data = context.getConfiguration().get("cloudflow.steps.map");
		try {
			steps = new SerializableSteps<IMapStep>();
			steps.load(data);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		List<Record> records = new Vector<>();
		records.add(new Record(key.toString(), value.toString()));

		try {
			// execute steps
			for (int i = 0; i < steps.getSize(); i++) {
				IMapStep step = steps.getStepInstance(i);
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
