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

	public GracefullShutdownParticle(long timestamp, String originId) {
		super(timestamp);
		setOriginId(originId);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GracefullShutdownParticle other = (GracefullShutdownParticle) obj;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "_GracefullShutdown[ " + originId + "," + timestamp + "]";
	}

}
