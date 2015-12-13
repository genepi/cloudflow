package cloudflow.core.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import cloudflow.bio.bam.BamRecord;
import cloudflow.bio.bam.BamWritableRecord;
import cloudflow.core.Pipeline;
import cloudflow.core.PipelineRunner;
import cloudflow.core.hadoop.records.FloatWritableRecord;
import cloudflow.core.hadoop.records.IWritableRecord;
import cloudflow.core.hadoop.records.IntegerWritableRecord;
import cloudflow.core.hadoop.records.TextWritableRecord;
import cloudflow.core.records.FloatRecord;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.records.Record;
import cloudflow.core.records.TextRecord;

public class MapReduceRunner extends PipelineRunner {

	private static Map<Class, Class> writableRecords = new HashMap<Class, Class>();

	public static void registerWritableRecord(
			Class<? extends Record<?, ?>> recordClass,
			Class<? extends IWritableRecord> writableRecordClass) {
		System.out.println("Record: " + recordClass.getName());
		writableRecords.put(recordClass, writableRecordClass);
	}

	
	static{
		
		registerWritableRecord(IntegerRecord.class, IntegerWritableRecord.class);
		registerWritableRecord(FloatRecord.class, FloatWritableRecord.class);
		registerWritableRecord(TextRecord.class, TextWritableRecord.class);
		registerWritableRecord(BamRecord.class, BamWritableRecord.class);

	}
	public static IWritableRecord createWritableRecord(
			Class<? extends Record<?, ?>> recordClass) {

		Class<? extends IWritableRecord> writableRecordClass = writableRecords
				.get(recordClass);
		if (writableRecordClass != null) {
			try {
				return (IWritableRecord) writableRecordClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		//return null;
		throw new NotImplementedException("no support for hadoop for record " + recordClass.getName() + " implemented!");

	}

	public boolean run(Pipeline pipeline) throws IOException {

		// TODO: check compatibility: output record step n = input record step n
		// +1

		if (!pipeline.check()) {
			return false;
		}

		GenericJob job = new GenericJob(pipeline.getName());
		job.setInput(pipeline.getInput());
		job.setOutput(pipeline.getOutput());
		job.setDriverClass(pipeline.getDriverClass());

		if (!(pipeline.getLoader() instanceof HadoopRecordFileLoader)) {
			System.out
					.println("Input loader doesn't support MapReduce Hadoop.");
			return false;
		}

		HadoopRecordFileLoader hadoopLoader = (HadoopRecordFileLoader) pipeline
				.getLoader();

		job.setInputFormat(hadoopLoader.getInputFormat());
		job.setMapOperations(pipeline.getMapOperations());
		job.setAfterReduceOperations(pipeline.getAfterReduceOperations());
		job.setMapperInputRecords(pipeline.getLoader().getRecordClass());
		job.setCombinerOperations(pipeline.getCombinerOperations());
		hadoopLoader.configure(job.getConfiguration());
		job.getConfiguration().set("cloudflow.loader",
				hadoopLoader.getClass().getName());

		// distribute configuration
		pipeline.getConf().writeToConfiguration(job.getConfiguration());

		job.setMapperOutputRecords(pipeline.getMapperOutputRecordClass());

		job.setMapperOutputRecordsKey(hadoopLoader.getInputKeyClass());
		job.setMapperOutputRecordsValue(hadoopLoader.getInputValueClass());

		System.out.println("Mapper output records: "
				+ pipeline.getMapperOutputRecordClass().getName() + "  ("
				+ hadoopLoader.getInputKeyClass().getName() + ", "
				+ hadoopLoader.getInputValueClass().getName() + ")");

		job.setReduceOperations(pipeline.getReduceOperations());
		return job.execute();

	}

}
