package cloudflow.hadoop.test;

import java.io.IOException;
import java.util.Vector;

import cloudflow.hadoop.old.Binary;
import cloudflow.hadoop.old.IForEachChunk;
import cloudflow.hadoop.old.ISplitter;

public class Pipeline {

	private String input;

	private String output;

	private SerializableSteps<IMapStep> mapSteps;

	private SerializableSteps<IReduceStep> reduceSteps;

	private String name;

	private ILoader loader;

	private Class driverClass;

	public Pipeline(String name, Class driverClass) {
		this.driverClass = driverClass;
		this.name = name;
		mapSteps = new SerializableSteps<IMapStep>();
		reduceSteps = new SerializableSteps<IReduceStep>();
	}

	public void load(String hdfs, ILoader loader) {
		this.input = hdfs;
		this.loader = loader;
	}

	public void split(ISplitter splitter) {

	}

	public void forEachLine(IForEachChunk forEach) {

	}

	public void distribute(Binary binary) {

	}

	public void addMapStep(Class step) {
		mapSteps.addStep(step);
	}

	public void addReduceStep(Class step) {
		reduceSteps.addStep(step);
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
