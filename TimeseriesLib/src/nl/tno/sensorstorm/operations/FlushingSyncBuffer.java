package nl.tno.sensorstorm.operations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.particles.MetaParticle;
import nl.tno.sensorstorm.particles.Particle;
import nl.tno.sensorstorm.particles.gracefullshutdown.GracefullShutdownParticle;

/**
 * Special type of {@link SyncBuffer} which flushes itself after receiving a
 * {@link GracefullShutdownParticle} from all it's sources.
 * 
 * This SyncBuffer learns who it's sources are. When it received a
 * {@link GracefullShutdownParticle} from all it's sources, the pushParticle
 * will behave like a flush.
 */
public class FlushingSyncBuffer extends SyncBuffer {

	private final Map<String, Boolean> receivedShutdownFromOrigins = new HashMap<String, Boolean>();

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
