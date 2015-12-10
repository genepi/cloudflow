package cloudflow.core.records;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class Record<KEY extends WritableComparable<?>, VALUE extends Writable> {

}
