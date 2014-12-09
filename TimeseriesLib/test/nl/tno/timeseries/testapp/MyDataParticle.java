package nl.tno.timeseries.testapp;

import nl.tno.timeseries.mapper.annotation.TupleField;
import nl.tno.timeseries.particles.AbstractDataParticle;
import nl.tno.timeseries.particles.DataParticle;

public class MyDataParticle<T> extends AbstractDataParticle implements
		DataParticle {

	@TupleField
	private T value;
	@TupleField
	private String myId;

	public MyDataParticle() {
	}

	public MyDataParticle(String myId, long timestamp, T value) {
		super(timestamp);
		this.myId = myId;
		this.value = value;
	}

	public T getValue() {
		return value;
	}

	public String getMyId() {
		return myId;
	}

	@Override
	public String toString() {
		return "MyParticle[" + myId + "," + timestamp + "," + value + "]";
	}

}
