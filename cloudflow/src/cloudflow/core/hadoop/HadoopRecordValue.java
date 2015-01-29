package cloudflow.core.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class HadoopRecordValue implements Writable, Configurable {

	private Writable instance;
	private Configuration conf;

	public HadoopRecordValue() {
		this.conf = null;
	}

	public void set(Writable obj) {
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
			String className = conf.get("cloudflow.steps.map.output.value");
			Class clazz = Class.forName(className);
			instance = (Writable) clazz.newInstance();
			System.out.println("Create instance of " + className);
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}