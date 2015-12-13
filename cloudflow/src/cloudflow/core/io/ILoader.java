package cloudflow.core.io;

import java.io.Serializable;

public interface ILoader  extends Serializable{
	public Class<?> getRecordClass();

}
