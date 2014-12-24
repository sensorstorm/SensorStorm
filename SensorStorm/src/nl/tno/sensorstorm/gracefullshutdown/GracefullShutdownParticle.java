package nl.tno.sensorstorm.gracefullshutdown;

import nl.tno.sensorstorm.api.particles.AbstractMetaParticle;
import nl.tno.sensorstorm.api.particles.MetaParticle;

/**
 * The gracefullShutdownParticle is a meta particle indicating that the topology
 * must prepare itself for a shutdown. After this particle no other meta or data
 * particles will be sent.
 * 
 * @author waaijbdvd
 * 
 */
public class GracefullShutdownParticle extends AbstractMetaParticle implements
		MetaParticle {

	public GracefullShutdownParticle() {
	}

	public GracefullShutdownParticle(long timestamp, String originId) {
		super(timestamp);
		setOriginId(originId);
	}

	@Override
	public String toString() {
		return "_GracefullShutdown[ " + originId + "," + timestamp + "]";
	}

	@Override
	public boolean equalMetaParticle(MetaParticle other) {
		if (this == other) {
			return true;
		}
		if (other == null) {
			return false;
		}
		if (getClass() != other.getClass()) {
			return false;
		}
		if (timestamp != other.getTimestamp()) {
			return false;
		}
		return true;
	}

}
