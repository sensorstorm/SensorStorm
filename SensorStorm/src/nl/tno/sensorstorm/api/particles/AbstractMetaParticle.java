package nl.tno.sensorstorm.api.particles;

/**
 * A default implementation of a {@link MetaParticle} (for convenience).
 */
public abstract class AbstractMetaParticle extends AbstractParticle implements
		MetaParticle {

	protected String originId;

	/**
	 * Empty constructor.
	 */
	public AbstractMetaParticle() {
	}

	/**
	 * Create a new AbstractParticle with the given originId and timestamp.
	 * 
	 * @param timestamp
	 *            Timestamp to be used
	 */
	public AbstractMetaParticle(long timestamp) {
		super(timestamp);
	}

	@Override
	public String getOriginId() {
		return originId;
	}

	@Override
	public void setOriginId(String originId) {
		this.originId = originId;
	}

	@Override
	public String toString() {
		return "_MP[" + originId + ", " + timestamp + "]";
	}

	@Override
	public boolean equalMetaParticle(MetaParticle other) {
		return (other != null) && getClass().equals(other.getClass())
				&& (timestamp == other.getTimestamp());
	}

}
