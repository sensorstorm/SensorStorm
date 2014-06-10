package nl.tno.timeseries.particles;

import nl.tno.timeseries.interfaces.Particle;

/**
 * A default implementation of a {@link Particle} (for convenience).
 */
public abstract class AbstractParticle implements Particle {

	protected String channelId;
	protected long sequenceNr;
	
	public AbstractParticle() {
	}
	
	public AbstractParticle(String channelId, long sequenceNr) {
		this.channelId = channelId;
		this.sequenceNr = sequenceNr;
	}
	

	@Override
	public String getChannelId() {
		return channelId;
	}

	@Override
	public void setChannelId(String streamId) {
		this.channelId = streamId;
	}

	@Override
	public long getSequenceNr() {
		return sequenceNr;
	}

	@Override
	public void setSequenceNr(long sequenceNr) {
		this.sequenceNr = sequenceNr;
	}
	
	
	@Override
	public String toString() {
		return "P["+channelId+","+sequenceNr+"]";
	}
}
