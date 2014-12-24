package nl.tno.sensorstorm.particles;

/**
 * A default implementation of a {@link MetaParticle} (for convenience).
 */
public abstract class AbstractMetaParticle extends AbstractParticle implements
		MetaParticle {

	protected String originId;

	/**
	 * Empty constructor
	 */
	public AbstractMetaParticle() {
	}

	/**
	 * Create a new AbstractParticle with the given originId and timestamp.
	 * 
	 * @param originId
	 * @param timestamp
	 */
	public AbstractMetaParticle(long timestamp) {
		super(timestamp);
		// TODO originId bepalen
		this.originId = "";
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

}
