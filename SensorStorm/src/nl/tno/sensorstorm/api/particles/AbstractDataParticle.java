package nl.tno.sensorstorm.api.particles;

/**
 * A default implementation of a {@link Particle} (for convenience).
 */
public abstract class AbstractDataParticle extends AbstractParticle implements
		DataParticle {

	/**
	 * Empty constructor
	 */
	public AbstractDataParticle() {
	}

	/**
	 * Create a new AbstractParticle with the given timestamp.
	 * 
	 * @param timestamp
	 */
	public AbstractDataParticle(long timestamp) {
		super(timestamp);
	}

	@Override
	public String toString() {
		return "DataParticle[" + timestamp + "]";
	}

}
