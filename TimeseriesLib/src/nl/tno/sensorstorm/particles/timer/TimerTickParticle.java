package nl.tno.sensorstorm.particles.timer;

import nl.tno.sensorstorm.particles.AbstractMetaParticle;
import nl.tno.sensorstorm.particles.MetaParticle;

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

}
