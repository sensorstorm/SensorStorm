package nl.tno.timeseries.particles.timer;

import nl.tno.timeseries.particles.AbstractMetaParticle;
import nl.tno.timeseries.particles.MetaParticle;

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
