package cloudflow.core.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import cloudflow.core.Operations;
import cloudflow.core.PipelineConf;
import cloudflow.core.hadoop.records.IWritableRecord;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.Record;
import cloudflow.core.records.RecordList;

public class GenericMapper extends
		Mapper<Object, Writable, HadoopRecordKey, HadoopRecordValue> {

	private Operations<Transformer<Record<?, ?>, Record<?, ?>>> steps;

	private List<Transformer<Record<?, ?>, Record<?, ?>>> instances = new Vector<>();

	private RecordList inputRecords = new RecordList();

	private IWritableRecord writableRecord;

	private static final Logger log = Logger.getLogger(GenericMapper.class);

	@Override
	public void run(Context context) throws IOException, InterruptedException {

		try {

			log.info("Loading Map Operations...");

			// read mapper steps
			String data = context.getConfiguration().get("cloudflow.steps.map");
			steps = new Operations<Transformer<Record<?, ?>, Record<?, ?>>>();
			steps.load(data);

			instances = steps.createInstances(inputRecords,
					new RecordToContextWriter(context));

			PipelineConf conf = new PipelineConf();
			conf.loadFromConfiguration(context.getConfiguration());

			// configure steps
			for (int i = 0; i < instances.size(); i++) {
				instances.get(i).configure(conf);
			}

			log.info("Found " + instances.size() + " map operations.");

		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new IOException(e);
		}

		// create record
		try {
			String inputRecordClassName = context.getConfiguration().get(
					"cloudflow.steps.map.input");

			log.info("Input Records are " + inputRecordClassName);

			Class<? extends Record<?, ?>> recordClass = (Class<? extends Record<?, ?>>) Class
					.forName(inputRecordClassName);

			writableRecord = MapReduceRunner.createWritableRecord(recordClass);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}

		while (context.nextKeyValue()) {

			// Fill record with values

			Record record = writableRecord.fillRecord(
					(WritableComparable) context.getCurrentKey(),
					context.getCurrentValue());

			inputRecords.add(record);

		}

	}
}
