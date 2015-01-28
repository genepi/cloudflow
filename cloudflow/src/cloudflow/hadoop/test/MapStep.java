package cloudflow.hadoop.test;

import java.util.List;

public abstract class MapStep {

	public abstract List<Record>  process(List<Record> records);

}
