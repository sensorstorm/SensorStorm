package nl.tno.timeseries.testapp;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.particles.AbstractParticle;

public class Measurement extends AbstractParticle implements DataParticle {

	double value;
	
	public Measurement() {
		
	}
	
	public Measurement(String channelId, long timestamp, double value) {
		setChannelId(channelId);
		setSequenceNr(timestamp);
		this.value = value;
	}
	
	public long getTimestamp() {
		return getSequenceNr();
	}
	
	public double getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return "P["+channelId+","+sequenceNr+","+value+"]";
	}
	
}
