package nl.tno.timeseries.channels;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.FaultTolerant;
import nl.tno.timeseries.interfaces.Particle;
import backtype.storm.tuple.Tuple;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Utility class used to keep track of {@link DataParticle} objects consumed by
 * Batchers and is used for fault tolerance (acking and failing). Batchers
 * <i><b>must remove the particles they do not need anymore</b></i> from the
 * cache or else they will timeout at some point which will trigger a fail.
 * 
 * @author Corne Versloot
 *
 */
public class ParticleCache implements RemovalListener<DataParticle, Tuple> {

	private final Cache<DataParticle, Tuple> cache;
	private final FaultTolerant ackFailDelegator;

	public ParticleCache(int maxSize, int timeoutSeconds,
			FaultTolerant ackFailDelegator) {
		cache = CacheBuilder.newBuilder().maximumSize(maxSize)
				.expireAfterWrite(timeoutSeconds, TimeUnit.SECONDS)
				.removalListener(this).build();
		this.ackFailDelegator = ackFailDelegator;
	}

	@Override
	public void onRemoval(RemovalNotification<DataParticle, Tuple> notification) {
		if (notification.getCause() == RemovalCause.EXPIRED
				|| notification.getCause() == RemovalCause.SIZE) {
			ackFailDelegator.fail(notification.getValue());
		} else {
			ackFailDelegator.ack(notification.getValue());
		}
	}

	/**
	 * Adds the particle and tuple combination to the cache. This function is
	 * typically used by Bolts to cache received data
	 * 
	 * @param particle
	 * @param tuple
	 */
	public void put(DataParticle particle, Tuple tuple) {
		cache.put(particle, tuple);
	}

	/**
	 * @return all cached particles as an unordered set
	 */
	public Set<DataParticle> getParticles() {
		return cache.asMap().keySet();
	}

	/**
	 * Removes the particle and triggers an ack
	 * 
	 * @param particle
	 */
	public void remove(Particle particle) {
		cache.invalidate(particle);
	}

	/**
	 * Removes all particles from the cache
	 */
	public void removeAll() {
		cache.invalidateAll();
	}

	/**
	 * Checks if the particle is present in the cache
	 * 
	 * @param particle
	 * @return true if the particle is present in the cache, else false
	 */
	public boolean contains(DataParticle particle) {
		return cache.getIfPresent(particle) != null;
	}
}
