package cloudflow.core.spark;

import java.io.Serializable;

import cloudflow.core.records.IRecordConsumer;
import cloudflow.core.records.Record;

public class RecordToListWriter2 implements IRecordConsumer<Record<?, ?>>, Serializable {

	private StringBuffer string = new StringBuffer();
	
	private boolean first = true;
	
	@Override
	public void consume(Record<?, ?> record) {
		if (first){
			string.append(record.toString());
			first = false;
		}else{
			string.append("\n");
			string.append(record.toString());
		}
	}

	public String toString() {
		return string.toString();
	}
}
