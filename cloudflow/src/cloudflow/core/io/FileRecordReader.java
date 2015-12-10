package cloudflow.core.io;

import cloudflow.core.records.Record;

public abstract class FileRecordReader<r extends Record<?, ?>> {

	public abstract boolean open(String filename);

	public abstract r next();

	public abstract void close();

}
