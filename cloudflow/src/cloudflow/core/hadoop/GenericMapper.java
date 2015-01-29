package cloudflow.core.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import cloudflow.core.SerializableSteps;
import cloudflow.core.operations.MapStep;
import cloudflow.core.records.Record;

public class GenericMapper extends
		Mapper<Object, Writable, HadoopRecordKey, HadoopRecordValue> {

	private SerializableSteps<MapStep<Record<?, ?>, Record<?, ?>>> steps;

	private List<MapStep<Record<?, ?>, Record<?, ?>>> instances = new Vector<>();

	private RecordList inputRecords = new RecordList();

	private Record<?, ?> record = null;

	private static final Logger log = Logger
			.getLogger(GenericMapper.class);
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {

		try {

			log.info("Loading Map Steps...");
			
			// read mapper steps
			String data = context.getConfiguration().get("cloudflow.steps.map");
			steps = new SerializableSteps<MapStep<Record<?, ?>, Record<?, ?>>>();
			steps.load(data);

			instances = steps.createInstances();

			log.info("Found " + instances.size() + " map steps.");
			
			// fist step consumes input records
			inputRecords.addConsumer(instances.get(0));

			// step n + 1 consumes records produced by n
			for (int i = 0; i < instances.size() - 1; i++) {
				MapStep<Record<?, ?>, Record<?, ?>> step = instances.get(i);
				MapStep<Record<?, ?>, Record<?, ?>> nextStep = instances
						.get(i + 1);
				step.getOutputRecords().addConsumer(nextStep);
			}

			// last step writes records to context
			instances.get(instances.size() - 1).getOutputRecords()
					.addConsumer(new RecordToContextWriter(context));

		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new IOException(e);
		}

		// create record
		try {
			String inputRecordClassName = context.getConfiguration().get(
					"cloudflow.steps.map.input");
			
			log.info("Input Records are " + inputRecordClassName);
			
			Class<?> recordClass = Class.forName(inputRecordClassName);
			record = (Record<?, ?>) recordClass.newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new IOException(e);
		}

		while (context.nextKeyValue()) {

			// Fill record with values

			record.setWritableKey((WritableComparable) context.getCurrentKey());
			record.setWritableValue(context.getCurrentValue());

			inputRecords.add(record);

		}

	}

}
