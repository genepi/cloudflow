package cloudflow.core.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.records.Record;
import cloudflow.core.records.RecordList;

public class SparkRunner extends PipelineRunner implements Serializable {

	private Broadcast<Pipeline> pipelineBroadcast;

	PairFlatMapFunction PSEUDO_MAPPER = new PairFlatMapFunction<Tuple2<WritableComparable, Writable>, String, Object>() {

		@Override
		public Iterable<Tuple2<String, Object>> call(
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

			SparkGroupedRecords<Record<?, ?>> recordValues = new SparkGroupedRecords<Record<?, ?>>(
					pipeline.getMapperOutputRecordClass());

			// read reduce step

			List<Summarizer<Record<?, ?>, Record<?, ?>>> instancesReduce = pipeline
					.getReduceOperations().createInstances();

			reduceStep = instancesReduce.get(0);

			// read filter steps

			filterSteps = pipeline.getAfterReduceOperations();

			RecordToListWriter2 listWriter = new RecordToListWriter2();

			filterSteps.createInstances(reduceStep.getOutputRecords(),
					listWriter);

			recordValues.setKey(key);
			recordValues.setValues(paramT1._2().iterator());
			reduceStep.summarize(key.toString(), recordValues);

			return listWriter.toString();
		}

	};

	Function PSEUDO_REDUCER_WITHOUT_REDUCER = new Function<Tuple2<String, Integer>, Object>() {

		@Override
		public Object call(Tuple2<String, Integer> tupel) throws Exception {

			Pipeline pipeline = pipelineBroadcast.getValue();

			RecordList inputRecords = new RecordList();

			RecordToListWriter2 listWriter = new RecordToListWriter2();

			try {
				pipeline.getAfterReduceOperations().createInstances(
						inputRecords, listWriter);
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}

			IntegerRecord record = new IntegerRecord();
			record.setKey(tupel._1());
			record.setValue(tupel._2());
			inputRecords.add(record);

			return listWriter.toString();
		}
	};

	private String master = "";

	public SparkRunner(String master) {
		this.master = master;
	}

	@Override
	public boolean run(Pipeline pipeline) throws IOException {

		pipeline.check();

		if (!(pipeline.getLoader() instanceof HadoopRecordFileLoader)) {
			System.out.println("Input loader doesn't support local files.");
			return false;
		}

		SparkConf conf = new SparkConf().setAppName("cloudflow-pipeline");
		JavaSparkContext context = new JavaSparkContext(conf);

		HadoopRecordFileLoader loader = ((HadoopRecordFileLoader) pipeline
				.getLoader());
		Configuration hadoopConf = new Configuration();

		pipelineBroadcast = context.broadcast(pipeline);

		final JavaPairRDD<Record, Record> inputPairRDD = context
				.newAPIHadoopFile(pipeline.getInput(), loader.getInputFormat(),
						loader.getInputKeyClass(), loader.getInputValueClass(),
						hadoopConf);

		final JavaRDD<?> values;

		//if (!pipeline.hasCountOperation()) {

			values = inputPairRDD.flatMapToPair(PSEUDO_MAPPER).groupByKey()
					.map(PSEUDO_REDUCER);
		/*} else {

			System.out
					.println("Count operation detected. switching to sparks internal version.");

			values = inputPairRDD.flatMapToPair(PSEUDO_MAPPER)
					.reduceByKey(new Function2<Integer, Integer, Integer>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Integer call(Integer i1, Integer i2) {
							return i1 + i2;
						}
					}).map(PSEUDO_REDUCER_WITHOUT_REDUCER);

		}*/

		// TODO: sorted keys needed?
		/*
		 * final JavaRDD<?> values = inputPairRDD.flatMapToPair(PSEUDO_MAPPER)
		 * .groupByKey().sortByKey().map(PSEUDO_REDUCER);
		 */

		values.saveAsTextFile(pipeline.getOutput());

		return true;
	}

}
