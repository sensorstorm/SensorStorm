package nl.tno.sensorstorm.timer;

import nl.tno.sensorstorm.api.particles.AbstractMetaParticle;
import nl.tno.sensorstorm.api.particles.MetaParticle;

/**
 * A specific MetaParticle about time.
 * 
 * @author waaijbdvd
 * 
 */
public class TimerTickParticle extends AbstractMetaParticle implements
		MetaParticle {

	public TimerTickParticle() {
	}

	public TimerTickParticle(long timestamp) {
		super(timestamp);
	}

	@Override
	public String toString() {
		return "_TimerTick[" + originId + "," + timestamp + "]";
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
