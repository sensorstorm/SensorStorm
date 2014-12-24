package nl.tno.sensorstorm.api.particles;

import nl.tno.sensorstorm.api.processing.MetaParticleHandler;
import nl.tno.sensorstorm.impl.SyncBuffer;
import nl.tno.sensorstorm.storm.SensorStormBolt;
import nl.tno.sensorstorm.storm.SensorStormSpout;

/**
 * A marker interface that indicates this particle is a {@link MetaParticle}
 * (instead of a {@link DataParticle}). {@link MetaParticle}s are handled by
 * {@link MetaParticleHandler}s and are used to trigger special behavior.
 * 
 * {@link MetaParticle}s need to implement their own version of equals called
 * equalMetaParticle, which ignores the originId. This method is used by the
 * {@link SyncBuffer} in order to remove duplicate {@link MetaParticle}s.
 */
public interface MetaParticle extends Particle {

	/**
	 * @return originId, an unique identifier for a spout or bolt instance
	 */
	String getOriginId();

	/**
	 * Set the originId of the {@link MetaParticle}. This method should only be
	 * called by the {@link SensorStormSpout} or {@link SensorStormBolt}.
	 * 
	 * @param originId
	 *            unique identifier for a spout or bolt instance
	 */
	void setOriginId(String originId);

	/**
	 * Check if this MetaParticle is equal to another {@link Particle}.
	 * {@link MetaParticle}s need to implement their own equals method, which
	 * ignores the originId. This method is used by the {@link SyncBuffer} in
	 * order to remove duplicate {@link MetaParticle}s.
	 * 
	 * @param other
	 *            Other MetaParticle used for comparison
	 * @return True if this object is equal to another instance of a
	 *         MetaParticle, with the exception of the originId.
	 */
	boolean equalMetaParticle(MetaParticle other);
}
