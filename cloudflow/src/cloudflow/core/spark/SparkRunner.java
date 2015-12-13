package cloudflow.core.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import cloudflow.core.Operations;
import cloudflow.core.Pipeline;
import cloudflow.core.PipelineRunner;
import cloudflow.core.hadoop.HadoopRecordFileLoader;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.hadoop.records.IWritableRecord;
import cloudflow.core.operations.Summarizer;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.Record;
import cloudflow.core.records.RecordList;

public class SparkRunner extends PipelineRunner implements Serializable {

	private Broadcast<Pipeline> pipelineBroadcast;	

	PairFlatMapFunction PSEUDO_MAPPER = new PairFlatMapFunction<Tuple2<WritableComparable, Writable>, String, Object>() {

		@Override
		public Iterable<Tuple2<String,  Object>> call(
				Tuple2<WritableComparable, Writable> tupel) throws Exception {

			Pipeline pipeline = pipelineBroadcast.getValue();

			IWritableRecord writableRecord = MapReduceRunner
					.createWritableRecord((Class<Record<?, ?>>) pipeline
							.getLoader().getRecordClass());
			RecordList inputRecords = new RecordList();

			RecordToListWriter listWriter = new RecordToListWriter();

			try {
				pipeline.getMapOperations().createInstances(inputRecords,
						listWriter);
			} catch (InstantiationException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			inputRecords.add(writableRecord.fillRecord(tupel._1(), tupel._2()));

			return listWriter.getMemory();
		}
	};

	Function<Tuple2<String, Iterable<Object>>, Object> PSEUDO_REDUCER = new Function<Tuple2<String, Iterable<Object>>, Object>() {

		@Override
		public Object call(Tuple2<String, Iterable<Object>> paramT1)
				throws Exception {

			String key = paramT1._1();

			Pipeline pipeline = pipelineBroadcast.getValue();

			Operations<Transformer<Record<?, ?>, Record<?, ?>>> filterSteps;

			Summarizer<Record<?, ?>, Record<?, ?>> reduceStep;

			SparkGroupedRecords<Record<?, ?>> recordValues = new SparkGroupedRecords<Record<?, ?>>();

			List<Transformer<Record<?, ?>, Record<?, ?>>> instancesFilter = new Vector<Transformer<Record<?, ?>, Record<?, ?>>>();

			System.out.println("Input Records are "
					+ pipeline.getMapperOutputRecordClass().getName());

			System.out.println("Loading Reduce Step...");

			// read reduce step

			List<Summarizer<Record<?, ?>, Record<?, ?>>> instancesReduce = pipeline
					.getReduceOperations().createInstances();

			reduceStep = instancesReduce.get(0);

			System.out.println("Loading Map Steps...");

			// read filter steps

			filterSteps = pipeline.getAfterReduceOperations();

			RecordToListWriter2 listWriter = new RecordToListWriter2();

			instancesFilter = filterSteps.createInstances(
					reduceStep.getOutputRecords(), listWriter);

			Class<? extends Record<?, ?>> inputRecordClass = (Class<? extends Record<?, ?>>) pipeline
					.getMapperOutputRecordClass();

			//TODO: recordvalues should wrap value iterator
			List<Record> values = new Vector<Record>();
			for (Object value : paramT1._2()) {
				Record record = inputRecordClass.newInstance();
				record.setKey(key);
				record.setValue(value);
				values.add(record);

			}

			recordValues.setKey(key);
			recordValues.setValues(values.iterator());
			reduceStep.summarize(key.toString(), recordValues);

			return listWriter.toString();
		}

	};
	
	private String master = "";
	
	public SparkRunner(String master){
		this.master= master;
	}

	@Override
	public boolean run(Pipeline pipeline) throws IOException {

		pipeline.check();

		if (!(pipeline.getLoader() instanceof HadoopRecordFileLoader)) {
			System.out.println("Input loader doesn't support local files.");
			return false;
		}

		SparkConf conf = new SparkConf().setAppName(
				"PerSequenceQual.FastQ_PerSequenceQual_Job").setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		HadoopRecordFileLoader loader = ((HadoopRecordFileLoader) pipeline
				.getLoader());
		Configuration hadoopConf = new Configuration();

		pipelineBroadcast = context.broadcast(pipeline);

		final JavaPairRDD<Record, Record> inputPairRDD = context
				.newAPIHadoopFile(pipeline.getInput(), loader.getInputFormat(),
						loader.getInputKeyClass(), loader.getInputValueClass(),
						hadoopConf);

		final JavaRDD<?> values = inputPairRDD.flatMapToPair(PSEUDO_MAPPER)
				.groupByKey().sortByKey().map(PSEUDO_REDUCER);

		values.saveAsTextFile(pipeline.getOutput());

		return true;
	}

}
