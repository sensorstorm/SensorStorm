package nl.tno.timeseries.testapp;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.mapper.annotation.TupleField;
import nl.tno.timeseries.particles.AbstractParticle;

public class MeasurementT<T> extends AbstractParticle implements DataParticle {

	@TupleField
	T value;
	
	public MeasurementT() {
		
	}
	
	public MeasurementT(String channelId, long timestamp, T value) {
		setChannelId(channelId);
		setSequenceNr(timestamp);
		this.value = value;
	}
	
	public long getTimestamp() {
		return getSequenceNr();
	}
	
	public T getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return "M["+channelId+","+sequenceNr+","+value+"]";
	}
	
}
