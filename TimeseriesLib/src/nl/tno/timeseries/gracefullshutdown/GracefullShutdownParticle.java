package nl.tno.timeseries.gracefullshutdown;

import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.particles.AbstractParticle;

/**
 * The gracefullShutdownParticle is a meta particle indicating that the topology
 * must prepare itself for a shutdown. After this particle no other meta or data
 * particles will be sent.
 * 
 * @author waaijbdvd
 * 
 */
public class GracefullShutdownParticle extends AbstractParticle implements
		MetaParticle {

	public GracefullShutdownParticle() {
	}

	public GracefullShutdownParticle(String channelId, long timestamp) {
		super(channelId, timestamp);
	}

	@Override
	public String toString() {
		return "G[" + channelId + "," + timestamp + "]";
	}

}
