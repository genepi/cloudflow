package cloudflow.core;

import genepi.hadoop.HdfsUtil;

import java.io.Serializable;
import java.util.List;

import cloudflow.core.hadoop.HadoopRecordFileLoader;
import cloudflow.core.io.ILoader;
import cloudflow.core.io.TextLineLoader;
import cloudflow.core.io.TextLoader;
import cloudflow.core.operations.Concat;
import cloudflow.core.operations.Executor;
import cloudflow.core.operations.Filter;
import cloudflow.core.operations.LineSplitter;
import cloudflow.core.operations.Mean;
import cloudflow.core.operations.Sum;
import cloudflow.core.operations.Summarizer;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.Record;

public class Pipeline implements Serializable{

	private String input;

	private String output;

	private PipelineConf conf;

	private Operations<Transformer<Record<?, ?>, Record<?, ?>>> mapOperations;

	private Operations<Transformer<Record<?, ?>, Record<?, ?>>> afterReduceOperations;

	private Operations<Summarizer<Record<?, ?>, Record<?, ?>>> reduceOperations;

	private Operations<Summarizer<?, ?>> combinerOperations;

	private String name;

	private ILoader loader;

	private Class<?> mapperOutputRecordClass = null;

	private Class<?> driverClass;

	private boolean count = false;
	
	public Pipeline(String name, Class<?> driverClass) {
		this.driverClass = driverClass;
		this.name = name;
		mapOperations = new Operations<Transformer<Record<?, ?>, Record<?, ?>>>();
		reduceOperations = new Operations<Summarizer<Record<?, ?>, Record<?, ?>>>();
		combinerOperations = new Operations<Summarizer<?, ?>>();
		afterReduceOperations = new Operations<Transformer<Record<?, ?>, Record<?, ?>>>();

		conf = new PipelineConf();

	}

	public MapBuilder load(String hdfs, HadoopRecordFileLoader loader) {
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

	protected void addMapOperation(Class<? extends Transformer<?, ?>> operation) {
		mapOperations.add(operation);
	}

	protected void addAfterReduceOperation(
			Class<? extends Transformer<?, ?>> operation) {
		afterReduceOperations.add(operation);
	}

	protected void addReduceOperation(
			Class<? extends Summarizer<?, ?>> operation) {
		reduceOperations.add(operation);
	}

	protected void setCombinerOperation(
			Class<? extends Summarizer<?, ?>> operation) {
		combinerOperations.add(operation);
	}

	public String getName() {
		return name;
	}

	public Operations<Transformer<Record<?, ?>, Record<?, ?>>> getAfterReduceOperations() {
		return afterReduceOperations;
	}

	public Operations<Summarizer<?, ?>> getCombinerOperations() {
		return combinerOperations;
	}

	public PipelineConf getConf() {
		return conf;
	}

	public Class<?> getDriverClass() {
		return driverClass;
	}

	public String getInput() {
		return input;
	}

	public ILoader getLoader() {
		return loader;
	}

	public Operations<Transformer<Record<?, ?>, Record<?, ?>>> getMapOperations() {
		return mapOperations;
	}

	public Class<?> getMapperOutputRecordClass() {
		return mapperOutputRecordClass;
	}

	public String getOutput() {
		return output;
	}

	public Operations<Summarizer<Record<?, ?>, Record<?, ?>>> getReduceOperations() {
		return reduceOperations;
	}

	public class MapBuilder {

		protected Pipeline pipeline;

		public MapBuilder(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public MapBuilder apply(Class<? extends Transformer<?, ?>> operation) {
			addMapOperation(operation);
			return new MapBuilder(pipeline);
		}

		public MapBuilder filter(Class<? extends Filter<?>> operation) {
			return apply(operation);
		}

		public AfterReduceBuilder mean() {
			return groupByKey().apply(Mean.class);
		}

		public AfterReduceBuilder sum() {
			count = true;
			return groupByKey(Sum.class).apply(Sum.class);
		}

		public AfterReduceBuilder concat() {
			return groupByKey().apply(Concat.class);
		}

		public ReduceBuilder groupByKey() {
			return new ReduceBuilder(pipeline);
		}

		public ReduceBuilder groupByKey(
				Class<? extends Summarizer<?, ?>> operation) {
			setCombinerOperation(operation);
			return new ReduceBuilder(pipeline);
		}

		public void save(String hdfs) {
			output = hdfs;
		}

	}

	public class ReduceBuilder {

		protected Pipeline pipeline;

		public ReduceBuilder(Pipeline pipeline) {
			this.pipeline = pipeline;
		}

		public AfterReduceBuilder apply(
				Class<? extends Summarizer<?, ?>> operation) {
			addReduceOperation(operation);
			return new AfterReduceBuilder(pipeline);
		}

		public AfterReduceBuilder execute(Class<? extends Executor> operation) {
			addReduceOperation(operation);
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

		public AfterReduceBuilder apply(
				Class<? extends Transformer<?, ?>> operation) {
			addAfterReduceOperation(operation);
			return new AfterReduceBuilder(pipeline);
		}

		public AfterReduceBuilder filter(Class<? extends Filter<?>> operation) {
			return apply(operation);
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

	public void distributeArchive(String key, String filename) {
		String path = HdfsUtil.path("cloudflow-cache", key);
		HdfsUtil.put(filename, path);
		conf.distributeArchive(key, path);
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
			List<Transformer<Record<?, ?>, Record<?, ?>>> operations = mapOperations
					.createInstances();
			for (int i = 0; i < operations.size(); i++) {
				Transformer<?, ?> operation = operations.get(i);
				System.out.println("    (" + (i + 1) + ") "
						+ operation.getClass().getName());
				System.out.println("      input: "
						+ operation.getInputRecordClass());
				System.out.println("      output: "
						+ operation.getOutputRecordClass());

				mapperOutputRecordClass = operation.getOutputRecordClass();
				
				

			}
		} catch (InstantiationException | IllegalAccessException e) {
			System.out.println("Pipeline is not executable:");
			e.printStackTrace();
			return false;
		}
		if (reduceOperations.getSize() > 0) {

			System.out.println("  Reducer: ");
			try {
				List<Summarizer<Record<?, ?>, Record<?, ?>>> reducer = reduceOperations
						.createInstances();
				System.out.println("    (1) "
						+ reducer.get(0).getClass().getName());
				System.out.println("      input: "
						+ reducer.get(0).getInputRecordClass());
				System.out.println("      output: "
						+ reducer.get(0).getOutputRecordClass());
				List<Transformer<Record<?, ?>, Record<?, ?>>> operations = afterReduceOperations
						.createInstances();
				for (int i = 0; i < operations.size(); i++) {
					Transformer<?, ?> operation = operations.get(i);
					System.out.println("    (" + (i + 2) + ") "
							+ operation.getClass().getName());
					System.out.println("      input: "
							+ operation.getInputRecordClass());
					System.out.println("      output: "
							+ operation.getOutputRecordClass());
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
	
	public boolean hasCountOperation(){
		return count;
	}

}
