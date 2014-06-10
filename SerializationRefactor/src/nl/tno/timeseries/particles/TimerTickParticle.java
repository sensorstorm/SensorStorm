package nl.tno.timeseries.particles;

import nl.tno.timeseries.interfaces.MetaParticle;


public class TimerTickParticle extends AbstractParticle implements MetaParticle {
	
	public TimerTickParticle() {
	}
	
	public TimerTickParticle(String channelId, long timestamp) {
		super(channelId, timestamp);
	}
	
	public long getTimestamp() {
		return getSequenceNr();
	}
	
	public void setTimestamp(long timestamp) {
		setSequenceNr(timestamp);
	}
	
	
	@Override
	public String toString() {
		return "T["+channelId+","+sequenceNr+"]";
	}

}
