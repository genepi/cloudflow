package cloudflow.core.records;


public interface GroupedRecords<IN extends Record<?, ?>> {

	public boolean hasNextRecord();

	public IN getRecord();

}
