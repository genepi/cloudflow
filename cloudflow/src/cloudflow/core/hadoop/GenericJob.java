package cloudflow.core.hadoop;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HadoopJob;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import cloudflow.core.Operations;
import cloudflow.core.operations.Summarizer;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.Record;

public class GenericJob extends HadoopJob {

	private Class<InputFormat<?, ?>> inputFormat;

	private Class<?> driverClass;

	private boolean needReducer = false;

	private boolean needCombiner = false;

	public void setInputFormat(Class<InputFormat<?, ?>> inputFormat) {
		this.inputFormat = inputFormat;
	}

	public void setDriverClass(Class<?> driverClass) {
		this.driverClass = driverClass;
	}

	@Override
	public void setupJob(Job job) {

		job.setJarByClass(driverClass);
		job.setInputFormatClass(inputFormat);
		job.setMapperClass(GenericMapper.class);
		job.setMapOutputKeyClass(HadoopRecordKey.class);
		job.setMapOutputValueClass(HadoopRecordValue.class);
		if (needReducer) {
			job.setReducerClass(GenericReducer.class);
		} else {
			// map only
			job.setNumReduceTasks(0);
		}
		
		if (needCombiner) {
			job.setCombinerClass(GenericCombiner.class);
		}
		job.setSortComparatorClass(HadoopRecordKeyComparator.class);
	}

	@Override
	protected void setupDistributedCache(CacheStore cache) throws IOException {

		// cache.

	}

	public GenericJob(String name) throws IOException {
		super(name);
	}

	public void setMapOperations(Operations<Transformer<Record<?, ?>, Record<?, ?>>> steps) {
		set("cloudflow.steps.map", steps.serialize());
	}

	public void setAfterReduceOperations(Operations<Transformer<Record<?, ?>, Record<?, ?>>> steps) {
		set("cloudflow.steps.map2", steps.serialize());
	}

	public void setReduceOperations(Operations<Summarizer<Record<?, ?>, Record<?, ?>>> steps) {
		if (steps.getSize() > 0) {
			needReducer = true;
			set("cloudflow.steps.reduce", steps.serialize());
		}
	}

	public void setCombinerOperations(Operations<Summarizer<?, ?>> steps) {
		if (steps.getSize() > 0) {
			needCombiner = true;
			set("cloudflow.steps.combiner", steps.serialize());
		}
	}

	public void setMapperOutputRecords(Class<?> mapperOutputRecordClass) {
		set("cloudflow.steps.map.output", mapperOutputRecordClass.getName());
	}

	public void setMapperOutputRecordsValue(
			Class<?> mapperOutputRecordValueClass) {
		set("cloudflow.steps.map.output.value",
				mapperOutputRecordValueClass.getName());
	}

	public void setMapperOutputRecordsKey(Class<?> mapperOutputRecordKeyClass) {
		set("cloudflow.steps.map.output.key",
				mapperOutputRecordKeyClass.getName());
	}

	public void setMapperInputRecords(Class<?> mapperInputRecordClass) {
		set("cloudflow.steps.map.input", mapperInputRecordClass.getName());
	}

}
