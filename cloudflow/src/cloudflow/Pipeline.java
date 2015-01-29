package cloudflow;

import java.io.IOException;

import cloudflow.core.hadoop.GenericJob;
import cloudflow.core.operations.MapStep;
import cloudflow.core.operations.ReduceStep;
import cloudflow.core.records.Record;
import cloudflow.core.ILoader;
import cloudflow.core.SerializableSteps;

public class Pipeline {

	private String input;

	private String output;

	private SerializableSteps<MapStep<?, ?>> mapSteps;

	private SerializableSteps<MapStep<?, ?>> mapSteps2;

	private SerializableSteps<ReduceStep<?, ?>> reduceSteps;

	private Class<?> mapperOutputRecordClass;

	private String name;

	private ILoader loader;

	private Class<?> driverClass;

	public Pipeline(String name, Class<?> driverClass) {
		this.driverClass = driverClass;
		this.name = name;
		mapSteps = new SerializableSteps<MapStep<?, ?>>();
		reduceSteps = new SerializableSteps<ReduceStep<?, ?>>();
		mapSteps2 = new SerializableSteps<MapStep<?, ?>>();

	}

	public void load(String hdfs, ILoader loader) {
		this.input = hdfs;
		this.loader = loader;
	}

	protected void addMapStep(Class<? extends MapStep<?, ?>> step) {
		mapSteps.addStep(step);
	}

	protected void addMap2Step(Class<? extends MapStep<?, ?>> step) {
		mapSteps2.addStep(step);
	}

	protected void addReduceStep(Class<? extends ReduceStep<?, ?>> step) {
		reduceSteps.addStep(step);
	}

	public MapBuilder perform(Class<? extends MapStep<?, ?>> step,
			Class<? extends Record<?, ?>> mapperOutputRecordClass) {

		addMapStep(step);

		// TODO: remove mapperOutputRecordClass -> detect it auto.
		this.mapperOutputRecordClass = mapperOutputRecordClass;

		return new MapBuilder(this);
	}

	public class MapBuilder {

		private Pipeline pipeline;

		public MapBuilder(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public MapBuilder perform(Class<? extends MapStep<?, ?>> step,
				Class<? extends Record<?, ?>> mapperOutputRecordClass2) {

			addMapStep(step);

			// TODO: remove mapperOutputRecordClass -> detect it auto.
			mapperOutputRecordClass = mapperOutputRecordClass2;

			return new MapBuilder(pipeline);
		}

		public ReduceBuilder groupByKey() {
			return new ReduceBuilder(pipeline);
		}

	}

	public class ReduceBuilder {

		private Pipeline pipeline;

		public ReduceBuilder(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public MapBuilder2 perform(Class<? extends ReduceStep<?, ?>> step) {
			addReduceStep(step);
			return new MapBuilder2(pipeline);
		}

	}

	public class MapBuilder2 {

		private Pipeline pipeline;

		public MapBuilder2(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public MapBuilder2 perform(Class<? extends MapStep<?, ?>> step) {
			addMap2Step(step);
			return new MapBuilder2(pipeline);
		}

	}

	public void save(String hdfs) {
		this.output = hdfs;
	}

	public boolean run() throws IOException {

		//TODO: check compatibility: output record step n = input record step n +1
		
		GenericJob job = new GenericJob(name);
		job.setInput(input);
		job.setOutput(output);
		job.setDriverClass(driverClass);
		job.setInputFormat(loader.getInputFormat());
		job.setMapSteps(mapSteps);
		job.setMap2Steps(mapSteps2);
		job.setMapperInputRecords(loader.getRecordClass());
		job.setMapperOutputRecords(mapperOutputRecordClass);
		try {

			// TODO: without instance!

			Record<?, ?> record = (Record<?, ?>) mapperOutputRecordClass
					.newInstance();
			job.setMapperOutputRecordsKey(record.getWritableKeyClass());
			job.setMapperOutputRecordsValue(record.getWritableValueClass());

			System.out.println("Mapper output records: "
					+ mapperOutputRecordClass.getName() + "  ("
					+ record.getWritableKeyClass().getName() + ", "
					+ record.getWritableValueClass().getName() + ")");

			job.setReduceSteps(reduceSteps);
			return job.execute();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;

	}

}
