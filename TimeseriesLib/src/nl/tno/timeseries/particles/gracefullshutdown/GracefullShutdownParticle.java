package nl.tno.timeseries.particles.gracefullshutdown;

import nl.tno.timeseries.particles.AbstractMetaParticle;
import nl.tno.timeseries.particles.MetaParticle;

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

	public GracefullShutdownParticle(long timestamp) {
		super(timestamp);
	}

	@Override
	public String toString() {
		return "_GracefullShutdown[ " + originId + "," + timestamp + "]";
	}

}
