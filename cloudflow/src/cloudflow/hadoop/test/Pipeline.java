package cloudflow.hadoop.test;

import java.io.IOException;
import cloudflow.hadoop.old.DistributedFile;
import cloudflow.hadoop.old.IForEachChunk;
import cloudflow.hadoop.old.ISplitter;

public class Pipeline {

	private String input;

	private String output;

	private SerializableSteps<MapStep> mapSteps;

	private SerializableSteps<ReduceStep> reduceSteps;

	private String name;

	private ILoader loader;

	private Class driverClass;

	public Pipeline(String name, Class driverClass) {
		this.driverClass = driverClass;
		this.name = name;
		mapSteps = new SerializableSteps<MapStep>();
		reduceSteps = new SerializableSteps<ReduceStep>();
	}

	public void load(String hdfs, ILoader loader) {
		this.input = hdfs;
		this.loader = loader;
	}

	public void split(ISplitter splitter) {

	}

	public void forEachLine(IForEachChunk forEach) {

	}

	public void distribute(String key, DistributedFile file) {

	}

	protected void addMapStep(Class<? extends MapStep> step) {
		mapSteps.addStep(step);
	}

	protected void addReduceStep(Class<? extends ReduceStep> step) {
		reduceSteps.addStep(step);
	}

	public MapBuilder perform(Class<? extends MapStep> step) {
		addMapStep(step);
		return new MapBuilder(this);
	}

	public class MapBuilder {

		private Pipeline pipeline;

		public MapBuilder(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public MapBuilder perform(Class<? extends MapStep> step) {
			addMapStep(step);
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

		public ReduceBuilder perform(Class<? extends ReduceStep> step) {
			addReduceStep(step);
			return new ReduceBuilder(pipeline);
		}

	}

	public void save(String hdfs) {
		this.output = hdfs;
	}

	public boolean run() throws IOException {
		GenericJob job = new GenericJob(name);
		job.setInput(input);
		job.setOutput(output);

		job.setDriverClass(driverClass);
		job.setInputFormat(loader.getInputFormat());
		job.setMapSteps(mapSteps);
		job.setReduceSteps(reduceSteps);
		return job.execute();
	}

}
