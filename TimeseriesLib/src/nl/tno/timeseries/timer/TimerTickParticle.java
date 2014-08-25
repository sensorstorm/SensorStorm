package nl.tno.timeseries.timer;

import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.particles.AbstractParticle;

/**
 * A specific MetaParticle about time.
 * 
 * @author waaijbdvd
 * 
 */
public class TimerTickParticle extends AbstractParticle implements MetaParticle {

	public TimerTickParticle() {
	}

	public TimerTickParticle(String channelId, long timestamp) {
		super(channelId, timestamp);
	}

	@Override
	public String toString() {
		return "T[" + channelId + "," + timestamp + "]";
	}

}
