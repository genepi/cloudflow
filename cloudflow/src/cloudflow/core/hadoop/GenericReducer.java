package cloudflow.core.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import cloudflow.core.Operations;
import cloudflow.core.PipelineConf;
import cloudflow.core.operations.Transformer;
import cloudflow.core.operations.Summarizer;
import cloudflow.core.records.Record;

public class GenericReducer
		extends
		Reducer<HadoopRecordKey, HadoopRecordValue, HadoopRecordKey, HadoopRecordValue> {

	private Operations<Summarizer<Record<?, ?>, Record<?, ?>>> reduceSteps;

	private Operations<Transformer<Record<?, ?>, Record<?, ?>>> filterSteps;

	private Summarizer<Record<?, ?>, Record<?, ?>> reduceStep;

	private GroupedRecords<Record<?, ?>> recordValues;

	private List<Transformer<Record<?, ?>, Record<?, ?>>> instancesFilter = new Vector<Transformer<Record<?, ?>, Record<?, ?>>>();

	private static final Logger log = Logger.getLogger(GenericReducer.class);

	@Override
	public void setup(final Context context) throws IOException,
			InterruptedException {

		try {

			log.info("Loading Reduce Step...");

			// read reduce step
			String data = context.getConfiguration().get(
					"cloudflow.steps.reduce");
			reduceSteps = new Operations<Summarizer<Record<?, ?>, Record<?, ?>>>();
			reduceSteps.load(data);

			PipelineConf conf = new PipelineConf();
			conf.loadFromConfiguration(context.getConfiguration());

			List<Summarizer<Record<?, ?>, Record<?, ?>>> instancesReduce = reduceSteps
					.createInstances();
			reduceStep = instancesReduce.get(0);
			reduceStep.configure(conf);

			log.info("Loading Map Steps...");

			// read filter steps
			String dataMap = context.getConfiguration().get(
					"cloudflow.steps.map2");

			filterSteps = new Operations<Transformer<Record<?, ?>, Record<?, ?>>>();
			if (dataMap != null) {
				filterSteps.load(dataMap);
			}

			if (filterSteps.getSize() > 0) {

				log.info("Found 1 reduce step.");
				log.info("Found " + filterSteps.getSize() + " filter steps.");

				instancesFilter = filterSteps.createInstances();

				// configure steps
				for (int i = 0; i < instancesFilter.size(); i++) {
					instancesFilter.get(i).configure(conf);
				}

				// fist step consumes reduce step output records
				reduceStep.getOutputRecords().addConsumer(
						instancesFilter.get(0));

				// step n + 1 consumes records produced by n
				for (int i = 0; i < instancesFilter.size() - 1; i++) {
					Transformer<Record<?, ?>, Record<?, ?>> step = instancesFilter
							.get(i);
					Transformer<Record<?, ?>, Record<?, ?>> nextStep = instancesFilter
							.get(i + 1);
					step.getOutputRecords().addConsumer(nextStep);
				}

				// last step writes records to context
				instancesFilter.get(instancesFilter.size() - 1)
						.getOutputRecords()
						.addConsumer(new RecordToContextWriter(context));
			} else {

				log.info("Found 1 reduce step.");

				// reduce step writes records to context
				reduceStep.getOutputRecords().addConsumer(
						new RecordToContextWriter(context));

			}

		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new IOException(e);
		}

		// create recordValues for input record type

		String inputRecordClassName = context.getConfiguration().get(
				"cloudflow.steps.map.output");

		log.info("Input Records are " + inputRecordClassName);

		Class<? extends Record<?, ?>> inputRecordClass;
		try {
			inputRecordClass = (Class<? extends Record<?, ?>>) Class
					.forName(inputRecordClassName);

			recordValues = new GroupedRecords<Record<?, ?>>();
			recordValues.setRecordClassName(inputRecordClass);
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected void reduce(HadoopRecordKey key,
			Iterable<HadoopRecordValue> values, Context context)
			throws IOException, InterruptedException {

		recordValues.setKey(key);
		recordValues.setValues(values.iterator());
		reduceStep.summarize(key.toString(), recordValues);

	}

}
