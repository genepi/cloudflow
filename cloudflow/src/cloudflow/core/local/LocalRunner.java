package cloudflow.core.local;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import cloudflow.core.Operations;
import cloudflow.core.Pipeline;
import cloudflow.core.PipelineRunner;
import cloudflow.core.hadoop.HadoopGroupedRecords;
import cloudflow.core.hadoop.HadoopRecordKey;
import cloudflow.core.hadoop.HadoopRecordValue;
import cloudflow.core.io.FileRecordReader;
import cloudflow.core.io.LocalFileLoader;
import cloudflow.core.operations.Summarizer;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.Record;
import cloudflow.core.records.RecordList;

public class LocalRunner extends PipelineRunner {

	private List<Transformer<Record<?,?>, Record<?,?>>> instances = new Vector<>();

	private RecordList inputRecords = new RecordList();

	@Override
	public boolean run(Pipeline pipeline) throws IOException {

		pipeline.check();

		if (!(pipeline.getLoader() instanceof LocalFileLoader)) {
			System.out.println("Input loader doesn't support local files.");
			return false;
		}

		Map<HadoopRecordKey, List<HadoopRecordValue>> memory = map(pipeline);

		return reduce(pipeline, memory);
	}

	private Map<HadoopRecordKey, List<HadoopRecordValue>> map(Pipeline pipeline) {
		LocalFileLoader localFileLoader = (LocalFileLoader) pipeline
				.getLoader();

		FileRecordReader<?> reader = localFileLoader
				.createFileRecordReader(pipeline.getInput());

		RecordToMemoryWriter context = new RecordToMemoryWriter();
		context.setRecordClass(pipeline.getMapperOutputRecordClass());

		try {
			instances = pipeline.getMapOperations().createInstances(
					inputRecords, context);
		} catch (InstantiationException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Found " + instances.size() + " map operations.");

		// read each record and execute map steps
		Record record = reader.next();
		while (record != null) {
			inputRecords.add(record);
			record = reader.next();
		}
		reader.close();

		return context.getMemory();
	}

	private boolean reduce(Pipeline pipeline,
			Map<HadoopRecordKey, List<HadoopRecordValue>> memory) {

		Operations<Transformer<Record<?,?>, Record<?,?>>> filterSteps;

		Summarizer<Record<?,?>, Record<?,?>> reduceStep;

		HadoopGroupedRecords<Record<?,?>> recordValues = new HadoopGroupedRecords<Record<?,?>>();

		List<Transformer<Record<?,?>, Record<?,?>>> instancesFilter = new Vector<Transformer<Record<?,?>, Record<?,?>>>();

		try {

			// sort all keys, create records and execute reducer steps

			List<HadoopRecordKey> keys = new Vector<HadoopRecordKey>();
			for (HadoopRecordKey key : memory.keySet()) {
				keys.add(key);
			}
			Collections.sort(keys);

			System.out.println("Input Records are "
					+ pipeline.getMapperOutputRecordClass().getName());

			Class<? extends Record<?,?>> inputRecordClass;
			try {
				inputRecordClass = (Class<? extends Record<?,?>>) pipeline
						.getMapperOutputRecordClass();
				recordValues = new HadoopGroupedRecords<Record<?,?>>();				
				recordValues.setRecordClassName(inputRecordClass);
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
				return false;
			}

			System.out.println("Loading Reduce Step...");

			// read reduce step

			List<Summarizer<Record<?,?>, Record<?,?>>> instancesReduce = pipeline
					.getReduceOperations().createInstances();

			reduceStep = instancesReduce.get(0);

			System.out.println("Loading Map Steps...");

			// read filter steps

			filterSteps = pipeline.getAfterReduceOperations();

			RecordToFileWriter outputWriter = new RecordToFileWriter(
					pipeline.getOutput());

			instancesFilter = filterSteps.createInstances(
					reduceStep.getOutputRecords(), outputWriter);

			// execute reducer for each grouped key
			for (HadoopRecordKey key : keys) {
				List<HadoopRecordValue> values = memory.get(key);
				recordValues.setKey(key);
				recordValues.setValues(values.iterator());
				reduceStep.summarize(key.toString(), recordValues);
			}

			outputWriter.close();

		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

}
