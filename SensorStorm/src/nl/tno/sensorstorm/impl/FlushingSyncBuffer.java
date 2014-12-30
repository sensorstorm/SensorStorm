package nl.tno.sensorstorm.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.gracefullshutdown.GracefullShutdownParticle;

/**
 * Special type of {@link SyncBuffer} which flushes itself after receiving a
 * {@link GracefullShutdownParticle} from all it's sources.
 * <p>
 * This SyncBuffer learns who it's sources are. When it received a
 * {@link GracefullShutdownParticle} from all it's sources, the pushParticle
 * will behave like a flush.
 */
public class FlushingSyncBuffer extends SyncBuffer {

	private final Map<String, Boolean> receivedShutdownFromOrigins = new HashMap<String, Boolean>();

	/**
	 * Create a new {@link FlushingSyncBuffer} with a predefined buffer time. A
	 * value of 0 will effectively disable the buffer, and disable the filtering
	 * of duplicate {@link MetaParticle}s.
	 * 
	 * @param bufferSizeMs
	 *            Amount of time {@link Particle}s are kept in the buffer in
	 *            milliseconds
	 */
	public FlushingSyncBuffer(long bufferSizeMs) {
		super(bufferSizeMs);
	}

	@Override
	public List<Particle> pushParticle(Particle particle) {
		if (particle instanceof MetaParticle) {
			if (!receivedShutdownFromOrigins
					.containsKey(((MetaParticle) particle).getOriginId())) {
				receivedShutdownFromOrigins.put(
						((MetaParticle) particle).getOriginId(), false);
			}
			if (particle instanceof GracefullShutdownParticle) {
				receivedShutdownFromOrigins.put(
						((MetaParticle) particle).getOriginId(), true);
				if (shouldFlush()) {
					// Return everything in the buffer
					// This MetaParticle is not relevant anymore
					return flush();
				}
			}
		}
		return super.pushParticle(particle);
	}

	/**
	 * Check if this buffer has received a {@link GracefullShutdownParticle}
	 * form all known origins.
	 * 
	 * @return Should the buffer flush?
	 */
	private boolean shouldFlush() {
		for (Boolean b : receivedShutdownFromOrigins.values()) {
			if (!b) {
				return false;
			}
		}
		return true;
	}

	@Override
	public List<Particle> flush() {
		receivedShutdownFromOrigins.clear();
		return super.flush();
	}

}
