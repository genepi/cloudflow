package cloudflow.core.spark;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import scala.Tuple2;
import cloudflow.core.records.IRecordConsumer;
import cloudflow.core.records.Record;

public class RecordToListWriter implements IRecordConsumer<Record<?, ?>>, Serializable{

	private List<Tuple2<String, Object>> memory = new Vector<Tuple2<String, Object>>();

	public void clear() {
		memory = new Vector<Tuple2<String, Object>>();
	}

	@Override
	public void consume(Record<?, ?> record) {
		memory.add(new Tuple2<String, Object>(record.getKey().toString(),
				record.getValue()));
	}

	public List<Tuple2<String, Object>> getMemory() {
		return memory;
	}

}
