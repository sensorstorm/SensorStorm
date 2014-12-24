package nl.tno.sensorstorm.api.particles;

/**
 * A default implementation of a {@link Particle} (for convenience).
 */
public abstract class AbstractParticle implements Particle {

	protected long timestamp;

	/**
	 * Empty constructor
	 */
	public AbstractParticle() {
	}

	/**
	 * Create a new AbstractParticle with the given timestamp.
	 * 
	 * @param timestamp
	 */
	public AbstractParticle(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
