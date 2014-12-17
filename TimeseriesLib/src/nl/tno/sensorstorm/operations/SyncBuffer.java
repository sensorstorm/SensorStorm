package nl.tno.sensorstorm.operations;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import nl.tno.sensorstorm.particles.MetaParticle;
import nl.tno.sensorstorm.particles.Particle;

/**
 * Class that synchronizes {@link Particle}s coming from multiple (probably
 * parallel executing) sources and filters out duplicate {@link MetaParticle}s.
 * This buffer introduces a configurable delay.
 *
 */
public class SyncBuffer {

	private static class ParticleComparator implements Comparator<Particle> {

		@Override
		public int compare(Particle p1, Particle p2) {
			int comp = Long.compare(p1.getTimestamp(), p2.getTimestamp());
			if (comp == 0) {
				// Same timestamp
				if (p1 instanceof MetaParticle && p2 instanceof MetaParticle
						&& p1.equals(p2)) {
					// This will filter out duplicate MetaParticles
					return 0;
				} else {
					// Arbitrary ordering
					return System.identityHashCode(p1)
							- System.identityHashCode(p2);
				}
			}
			return comp;
		}

	}

	private final long bufferSizeMs;
	private long lastSentOutTimestamp;
	private final TreeSet<Particle> particles;

	public SyncBuffer(long bufferSizeMs) {
		this.bufferSizeMs = bufferSizeMs;
		lastSentOutTimestamp = -1;
		particles = new TreeSet<Particle>(new ParticleComparator());
	}

	public List<Particle> pushParticle(Particle particle) {
		if (particle != null) {
			if (particle.getTimestamp() < lastSentOutTimestamp) {
				// Reject
				// TODO We should probably indicate this with metrics
				throw new IllegalArgumentException(
						"Particle arrived too late, rejected by SyncBuffer");
			}
			particles.add(particle);
		}
		ArrayList<Particle> list = new ArrayList<Particle>(5);
		long target = highestTimestamp() - bufferSizeMs;
		while (lowestTimestamp() <= target) {
			list.add(particles.pollFirst());
		}
		if (!list.isEmpty()) {
			lastSentOutTimestamp = list.get(0).getTimestamp();
		}
		return list;
	}

	public List<Particle> flush() {
		ArrayList<Particle> list = new ArrayList<Particle>(this.size());
		list.addAll(particles);
		particles.clear();
		if (!list.isEmpty()) {
			lastSentOutTimestamp = list.get(0).getTimestamp();
		}
		return list;
	}

	public long highestTimestamp() {
		if (size() == 0) {
			return -1;
		}
		return particles.last().getTimestamp();
	}

	public long lowestTimestamp() {
		if (size() == 0) {
			return -1;
		}
		return particles.first().getTimestamp();
	}

	public int size() {
		return particles.size();
	}

}
