package cloudflow.hadoop.records;

import org.apache.hadoop.io.Text;

public class RecordValues {

	private Iterable<Text> values;

	public RecordValues(Iterable<Text> values) {
		this.values = values;
	}

	public RecordValues() {

	}

	public void setValues(Iterable<Text> values) {
		this.values = values;
	}

	public boolean next() {
		return values.iterator().hasNext();
	}

	public String value() {
		return values.iterator().next().toString();
	}

}
