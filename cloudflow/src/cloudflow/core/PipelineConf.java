package cloudflow.core;

import genepi.hadoop.CacheStore;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;

public class PipelineConf implements Serializable{

	private Map<String, Integer> intValues = new HashMap<String, Integer>();

	private Map<String, String> stringValues = new HashMap<String, String>();

	private Map<String, Boolean> booleanValues = new HashMap<String, Boolean>();

	private List<String> files = new Vector<String>();
	
	private HashMap<String,String> archives = new HashMap<String,String>();
	
	private CacheStore cacheStore;

	private Configuration conf;

	public PipelineConf() {

	}

	public void writeToConfiguration(Configuration conf) {
		this.conf = conf;
		cacheStore = new CacheStore(conf);
		for (String filename : files) {
			cacheStore.addFile(filename);
		}
		for (Map.Entry<String, String> entry : archives.entrySet()) {
			cacheStore.addArchive(entry.getKey(),entry.getValue());
		}
		for (String key : intValues.keySet()) {
			conf.setInt(key, intValues.get(key));
		}
		for (String key : stringValues.keySet()) {
			conf.set(key, stringValues.get(key));
		}
		for (String key : booleanValues.keySet()) {
			conf.setBoolean(key, booleanValues.get(key));
		}
	}

	public void loadFromConfiguration(Configuration conf) {
		cacheStore = new CacheStore(conf);
		this.conf = conf;
	}

	public void set(String key, String value) {
		stringValues.put(key, value);
	}

	public String get(String key) {
		return conf.get(key);
	}

	public void set(String key, int value) {
		intValues.put(key, value);
	}

	public void set(String key, boolean value) {
		booleanValues.put(key, value);
	}

	public void distributeFile(String filename) {
		files.add(filename);
	}
	
	public void distributeArchive(String key, String path) {
		archives.put(key, path);
	}

	public String getFile(String filename){
		try {
			return cacheStore.getFile(filename);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String getArchive(String filename){
		try {
			return cacheStore.getArchive(filename);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}


}
