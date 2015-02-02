package cloudflow.core;

import genepi.hadoop.HdfsUtil;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileUtil;

import cloudflow.core.hadoop.GenericJob;
import cloudflow.core.io.ILoader;
import cloudflow.core.io.TextLineLoader;
import cloudflow.core.io.TextLoader;
import cloudflow.core.operations.BinaryExecutor;
import cloudflow.core.operations.LineSplitter;
import cloudflow.core.operations.MapStep;
import cloudflow.core.operations.Mean;
import cloudflow.core.operations.ReduceStep;
import cloudflow.core.records.Record;

public class Pipeline {

	private String input;

	private String output;

	private PipelineConf conf;

	private SerializableSteps<MapStep<?, ?>> mapSteps;

	private SerializableSteps<MapStep<?, ?>> mapSteps2;

	private SerializableSteps<ReduceStep<?, ?>> reduceSteps;

	private String name;

	private ILoader loader;

	private Class<?> mapperOutputRecordClass = null;

	private Class<?> driverClass;

	public Pipeline(String name, Class<?> driverClass) {
		this.driverClass = driverClass;
		this.name = name;
		mapSteps = new SerializableSteps<MapStep<?, ?>>();
		reduceSteps = new SerializableSteps<ReduceStep<?, ?>>();
		mapSteps2 = new SerializableSteps<MapStep<?, ?>>();
		conf = new PipelineConf();

	}

	public MapBuilder load(String hdfs, ILoader loader) {
		this.input = hdfs;
		this.loader = loader;

		return new MapBuilder(this);

	}

	public MapBuilder loadText(String hdfs) {
		this.input = hdfs;
		this.loader = new TextLoader();

		return new MapBuilder(this);

	}

	public ReduceBuilder loadTextAndSplit(String hdfs, int numLines) {
		this.input = hdfs;
		this.loader = new TextLineLoader(numLines);
		return new MapBuilder(this).apply(LineSplitter.class).groupByKey();

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

	public class MapBuilder {

		private Pipeline pipeline;

		public MapBuilder(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public MapBuilder apply(Class<? extends MapStep<?, ?>> step) {

			addMapStep(step);
			return new MapBuilder(pipeline);
		}

		public AfterReduceBuilder mean() {
			return groupByKey().apply(Mean.class);
		}

		public ReduceBuilder groupByKey() {
			return new ReduceBuilder(pipeline);
		}

		public void save(String hdfs) {
			output = hdfs;
		}

	}

	public class ReduceBuilder {

		private Pipeline pipeline;

		public ReduceBuilder(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public AfterReduceBuilder apply(Class<? extends ReduceStep<?, ?>> step) {
			addReduceStep(step);
			return new AfterReduceBuilder(pipeline);
		}

		public AfterReduceBuilder execute(Class<? extends BinaryExecutor> step) {
			addReduceStep(step);
			return new AfterReduceBuilder(pipeline);
		}

		public void save(String hdfs) {
			output = hdfs;
		}

	}

	public class AfterReduceBuilder {

		private Pipeline pipeline;

		public AfterReduceBuilder(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public AfterReduceBuilder apply(Class<? extends MapStep<?, ?>> step) {
			addMap2Step(step);
			return new AfterReduceBuilder(pipeline);
		}

		public void save(String hdfs) {
			output = hdfs;
		}
	}

	public void set(String key, String value) {
		conf.set(key, value);
	}

	public void set(String key, int value) {
		conf.set(key, value);
	}

	public void set(String key, boolean value) {
		conf.set(key, value);
	}

	public void distributeFile(String key, String filename) {
		key = HdfsUtil.path("cloudflow-cache", key);
		HdfsUtil.put(filename, key);
		conf.distributeFile(key);
	}

	public boolean check() {

		System.out.println("Execution Plan: ");

		System.out.println("  Input: ");

		System.out.println("    " + loader.getClass().getName());
		System.out.println("      hdfs: " + input);
		System.out.println("      records: "
				+ loader.getRecordClass().getName());

		System.out.println("  Mapper: ");
		try {
			List<MapStep<?, ?>> steps = mapSteps.createInstances();
			for (int i = 0; i < steps.size(); i++) {
				MapStep<?, ?> step = steps.get(i);
				System.out.println("    (" + (i + 1) + ") "
						+ step.getClass().getName());
				System.out
						.println("      input: " + step.getInputRecordClass());
				System.out.println("      output: "
						+ step.getOutputRecordClass());

				mapperOutputRecordClass = step.getOutputRecordClass();

			}
		} catch (InstantiationException | IllegalAccessException e) {
			System.out.println("Pipeline is not executable:");
			e.printStackTrace();
			return false;
		}
		if (reduceSteps.getSize() > 0) {

			System.out.println("  Reducer: ");
			try {
				List<ReduceStep<?, ?>> reducer = reduceSteps.createInstances();
				System.out.println("    (1) "
						+ reducer.get(0).getClass().getName());
				System.out.println("      input: "
						+ reducer.get(0).getInputRecordClass());
				System.out.println("      output: "
						+ reducer.get(0).getOutputRecordClass());
				List<MapStep<?, ?>> steps = mapSteps2.createInstances();
				for (int i = 0; i < steps.size(); i++) {
					MapStep<?, ?> step = steps.get(i);
					System.out.println("    (" + (i + 2) + ") "
							+ step.getClass().getName());
					System.out.println("      input: "
							+ step.getInputRecordClass());
					System.out.println("      output: "
							+ step.getOutputRecordClass());
				}
			} catch (InstantiationException | IllegalAccessException e) {
				System.out.println("Pipeline is not executable:");
				e.printStackTrace();
				return false;
			}
		}

		System.out.println("  Output: ");
		System.out.println("      hdfs: " + output);

		if (mapperOutputRecordClass == null) {
			System.out
					.println("Pipeline is not executable: No mapper output record class found!");
			return false;
		}

		return true;

	}

	public boolean run() throws IOException {

		// TODO: check compatibility: output record step n = input record step n
		// +1

		if (!check()) {
			return false;
		}

		GenericJob job = new GenericJob(name);
		job.setInput(input);
		job.setOutput(output);
		job.setDriverClass(driverClass);
		job.setInputFormat(loader.getInputFormat());
		job.setMapSteps(mapSteps);
		job.setMap2Steps(mapSteps2);
		job.setMapperInputRecords(loader.getRecordClass());
		loader.configure(job.getConfiguration());

		// distribute configuration
		conf.writeToConfiguration(job.getConfiguration());

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
