package nl.tno.sensorstorm.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.api.processing.Operation;

/**
 * Class that synchronizes {@link Particle}s coming from multiple (probably
 * parallel executing) sources and filters out duplicate {@link MetaParticle}s.
 * This buffer introduces a configurable delay.
 * 
 * When the delay is 0 the buffer will always immediately return the same
 * particle. Warning: When the size is 0, duplicate MetaParticles will not be
 * filtered out!
 */
public class SyncBuffer {

	private static final int INITIAL_CAPACITY_RETURN_LIST = 5;

	/**
	 * Special {@link Comparator} used by the {@link SyncBuffer}. It sorts
	 * {@link Particle}s based on their timestamp. Additionally, duplicate
	 * {@link MetaParticle}s (with possibly a different originId) will return 0,
	 * which results in removing the duplicates from the TreeSet.
	 */
	private static class ParticleComparator implements Comparator<Particle> {

		@Override
		public int compare(Particle p1, Particle p2) {
			int comp = Long.compare(p1.getTimestamp(), p2.getTimestamp());
			if (comp == 0) {
				// Same timestamp
				if ((p1 instanceof MetaParticle)
						&& (p2 instanceof MetaParticle)
						&& ((MetaParticle) p1)
								.equalMetaParticle((MetaParticle) p2)) {
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
	private final TreeSet<Particle> particles;
	private long lastSentOutTimestamp;

	/**
	 * Create a new {@link SyncBuffer} with a predefined buffer time. A value of
	 * 0 will effectively disable the buffer, and disable the filtering of
	 * duplicate {@link MetaParticle}s.
	 * 
	 * @param bufferSizeMs
	 *            Amount of time {@link Particle}s are kept in the buffer in
	 *            milliseconds
	 */
	public SyncBuffer(long bufferSizeMs) {
		if (bufferSizeMs < 0) {
			throw new IllegalArgumentException("buffer size cannot be negative");
		}
		this.bufferSizeMs = bufferSizeMs;
		lastSentOutTimestamp = -1;
		particles = new TreeSet<Particle>(new ParticleComparator());
	}

	/**
	 * Push a new {@link Particle} in into the {@link SyncBuffer}. When pushing
	 * a {@link Particle}, zero or more {@link Particle}s will leave the buffer
	 * and can be processed by the {@link Operation}. When a {@link Particle} is
	 * too late to be synchronized it will be rejected.
	 * 
	 * @param particle
	 *            {@link Particle} to add to the {@link SyncBuffer}
	 * @return List of {@link Particle} that leave the buffer (can be empty)
	 * @throws IllegalArgumentException
	 *             When the {@link Particle} is too late to be synchronized in
	 *             the buffer
	 */
	public List<Particle> pushParticle(Particle particle) {
		if (bufferSizeMs == 0) {
			return Collections.singletonList(particle);
		}
		if (particle != null) {
			if (particle.getTimestamp() < lastSentOutTimestamp) {
				// Reject
				throw new IllegalArgumentException(
						"Particle arrived too late, rejected by SyncBuffer");
			}
			particles.add(particle);
		}
		ArrayList<Particle> list = new ArrayList<Particle>(
				INITIAL_CAPACITY_RETURN_LIST);
		long target = highestTimestamp() - bufferSizeMs;
		while (lowestTimestamp() <= target) {
			list.add(particles.pollFirst());
		}
		if (!list.isEmpty()) {
			lastSentOutTimestamp = list.get(0).getTimestamp();
		}
		return list;
	}

	/**
	 * Remove all elements from the {@link SyncBuffer}.
	 * 
	 * @return Sorted List of elements that were in the {@link SyncBuffer}
	 */
	public List<Particle> flush() {
		ArrayList<Particle> list = new ArrayList<Particle>(size());
		list.addAll(particles);
		particles.clear();
		if (!list.isEmpty()) {
			lastSentOutTimestamp = list.get(0).getTimestamp();
		}
		return list;
	}

	/**
	 * Return the timestamp of the youngest {@link Particle} in the
	 * {@link SyncBuffer}.
	 * 
	 * @return Highest timestamp of a {@link Particle} in the buffer
	 */
	public long highestTimestamp() {
		if (size() == 0) {
			return -1;
		}
		return particles.last().getTimestamp();
	}

	/**
	 * Return the timestamp of the oldest {@link Particle} in the
	 * {@link SyncBuffer}.
	 * 
	 * @return Lowest timestamp of a {@link Particle} in the buffer
	 */
	public long lowestTimestamp() {
		if (size() == 0) {
			return -1;
		}
		return particles.first().getTimestamp();
	}

	/**
	 * @return Number of {@link Particle}s in the buffer
	 */
	public int size() {
		return particles.size();
	}

}
