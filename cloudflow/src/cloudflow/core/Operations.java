package cloudflow.core;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import cloudflow.core.operations.IOperation;
import cloudflow.core.records.IRecordConsumer;
import cloudflow.core.records.IRecordProducer;

public class Operations<c extends IOperation> implements Serializable {

	private List<Class> steps;

	public int getSize() {
		return steps.size();
	}

	public c getStepInstance(int i) throws InstantiationException,
			IllegalAccessException {
		return (c) steps.get(i).newInstance();
	}

	public void add(Class clazz) {
		steps.add(clazz);
	}

	public Operations() {
		this.steps = new Vector<Class>();
	}

	public String serialize() {
		String data = "";
		for (Class step : steps) {
			data += step.getName() + " ";
		}

		return data;
	}

	public void load(String data) throws ClassNotFoundException {
		this.steps = new Vector<Class>();
		String[] tiles = data.split(" ");
		for (String tile : tiles) {
			steps.add(Class.forName(tile));
		}
	}

	public List<c> createInstances() throws InstantiationException,
			IllegalAccessException {

		return createInstances(null, null);

	}

	public List<c> createInstances(IRecordProducer producer,
			IRecordConsumer finalConsumer) throws InstantiationException,
			IllegalAccessException {

		List<c> instances = new Vector<c>();
		for (int i = 0; i < steps.size(); i++) {
			instances.add((c) steps.get(i).newInstance());
		}

		if (instances.size() > 0) {

			// fist step consumes input records from producer
			if (producer != null) {
				producer.addConsumer((IRecordConsumer) instances.get(0));
			}

			// step n + 1 consumes records produced by n
			for (int i = 0; i < instances.size() - 1; i++) {
				c step = instances.get(i);
				c nextStep = instances.get(i + 1);
				step.getOutputRecords().addConsumer((IRecordConsumer) nextStep);
			}

			// last step writes records to final consumer
			if (finalConsumer != null) {
				instances.get(instances.size() - 1).getOutputRecords()
						.addConsumer(finalConsumer);
			}

		} else {
			if (producer != null && finalConsumer != null) {
				producer.addConsumer(finalConsumer);
			}
		}

		return instances;
	}

}
