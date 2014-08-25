package nl.tno.timeseries.particles;

import nl.tno.timeseries.interfaces.Particle;

/**
 * A default implementation of a {@link Particle} (for convenience).
 */
public abstract class AbstractParticle implements Particle {

	protected String channelId;
	protected long timestamp;

	/**
	 * Empty constructor
	 */
	public AbstractParticle() {
	}

	/**
	 * Create a new AbstractParticle with the given channelId and timestamp.
	 * 
	 * @param channelId
	 * @param timestamp
	 */
	public AbstractParticle(String channelId, long timestamp) {
		this.channelId = channelId;
		this.timestamp = timestamp;
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
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "P[" + channelId + "," + timestamp + "]";
	}
}
