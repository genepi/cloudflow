package cloudflow.core.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableComparator;

//hadoops writablecomparator has a bug with setConf and getConf --> 
public class HadoopRecordKeyComparator extends WritableComparator {

	private HadoopRecordKey key1;
	private HadoopRecordKey key2;
	private DataInputBuffer buffer;

	private WritableComparator comparator = null;

	public HadoopRecordKeyComparator() {
		super(HadoopRecordKey.class, false);
		this.buffer = new DataInputBuffer();
	}

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		System.out.println("Config in Comparator!!");

		String className = conf.get("cloudflow.steps.map.output.key");
		Class clazz;
		try {
			// find comparator for key class
			clazz = Class.forName(className);
			comparator = WritableComparator.get(clazz);
			if (comparator != null) {
				System.out.println("Comparator found: "
						+ comparator.getClass().getName());
			} else {
				System.out.println("No Comparator found :(");
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		if (comparator != null) {
			// use fast byte comparator!
			return comparator.compare(b1, s1, l1, b2, s2, l2);
		} else {
			// use my slow comparator!
			System.out.println("fuck :(");
			if (key1 == null) {
				key1 = new HadoopRecordKey();
				key1.setConf(getConf());
			}

			if (key2 == null) {
				key2 = new HadoopRecordKey();
				key2.setConf(getConf());
			}

			try {
				this.buffer.reset(b1, s1, l1);
				this.key1.readFields(this.buffer);

				this.buffer.reset(b2, s2, l2);
				this.key2.readFields(this.buffer);

				this.buffer.reset(null, 0, 0);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			return compare(this.key1, this.key2);
		}
	}

}