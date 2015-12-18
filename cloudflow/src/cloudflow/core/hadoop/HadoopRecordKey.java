package cloudflow.core.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HadoopRecordKey implements WritableComparable<HadoopRecordKey>,
		Configurable {

	private WritableComparable instance;
	private Configuration conf;

	public HadoopRecordKey() {
		this.conf = null;
	}

	public void set(WritableComparable obj) {
		this.instance = obj;
	}

	public Writable get() {
		return this.instance;
	}

	public String toString() {
		return this.instance.toString();
	}

	public void readFields(DataInput in) throws IOException {
		this.instance.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.instance.write(out);
	}

	public Configuration getConf() {
		return this.conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		try {
			String className = conf.get("cloudflow.steps.map.output.key");
			System.out.println("Create instance of " + className);
			Class clazz = Class.forName(className);
			instance = (WritableComparable) clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int compareTo(HadoopRecordKey o) {
		return instance.compareTo(o.instance);
	}

	static {
		WritableComparator.define(HadoopRecordKey.class,
				new HadoopRecordKeyComparator());
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return instance.equals(((HadoopRecordKey)obj).instance);
	}
	
	@Override
	public int hashCode() {
		return instance.hashCode();
	}

}