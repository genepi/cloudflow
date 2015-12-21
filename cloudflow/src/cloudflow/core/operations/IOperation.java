package cloudflow.core.operations;

import java.io.Serializable;

import cloudflow.core.records.RecordList;

public interface IOperation extends Serializable{
	public RecordList getOutputRecords();
}
