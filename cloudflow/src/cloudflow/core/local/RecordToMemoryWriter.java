package cloudflow.core.local;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import cloudflow.core.hadoop.HadoopRecordKey;
import cloudflow.core.hadoop.HadoopRecordValue;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.hadoop.records.IWritableRecord;
import cloudflow.core.records.IRecordConsumer;
import cloudflow.core.records.Record;

public class RecordToMemoryWriter implements IRecordConsumer<Record<?, ?>> {

	private Map<HadoopRecordKey, List<HadoopRecordValue>> memory = new HashMap<>();

	private IWritableRecord writableRecord;
	
	public void setRecordClass(Class recordClass){
		writableRecord = MapReduceRunner.createWritableRecord(recordClass);
		System.out.println(recordClass.getName());
	}
	
	@Override
	public void consume(Record<?, ?> record) {
		
		if (writableRecord == null){
			System.out.println("OK");
		}
		
		WritableComparable newKey = WritableUtils.clone(writableRecord.fillWritableKey(record),
				new Configuration());
		Writable newValue = WritableUtils.clone(writableRecord.fillWritableValue(record),
				new Configuration());		
		HadoopRecordKey hadoopKey = new HadoopRecordKey(); 
		hadoopKey.set(newKey);
		HadoopRecordValue hadoopValue = new HadoopRecordValue();
		hadoopValue.set(newValue);
		
		List<HadoopRecordValue> list = memory.get(hadoopKey);
		if (list == null) {
			list = new Vector<HadoopRecordValue>();
			memory.put(hadoopKey, list);
		}
		list.add(hadoopValue);

	}

	public Map<HadoopRecordKey, List<HadoopRecordValue>> getMemory() {
		return memory;
	}

}
