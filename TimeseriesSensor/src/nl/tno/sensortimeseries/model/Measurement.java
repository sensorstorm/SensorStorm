package nl.tno.sensortimeseries.model;

import nl.tno.timeseries.model.Streamable;

public class Measurement extends Streamable {
	private final Object value;
	private final Class<?> valueClass;

	
	public Measurement(String sensorid, long timestamp, Object value, Class<?> valueClass) {
		super(sensorid, timestamp);
		this.value = value;
		this.valueClass = valueClass;
	}
	
	
	public String getSensorid() {
		return getStreamId();
	}

	public long getTimestamp() {
		return getSequenceNr();
	}
	
	
	public Object getValue() {
		return value;
	}

	
	public Class<?> getValueClass() {
		return valueClass;
	}
	
	public String toString() {
		return "["+getSequenceNr()+","+value+"]";
	}
	
	
}
