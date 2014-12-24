package nl.tno.sensorstorm.api.particles;

import java.util.ArrayList;
import java.util.Collection;

import nl.tno.sensorstorm.api.processing.Batcher;

/**
 * An extension of an {@link ArrayList} which forms a batch of
 * {@link DataParticle}s being created by a {@link Batcher}.
 */
public class DataParticleBatch extends ArrayList<DataParticle> {
	private static final long serialVersionUID = 4908173149847262415L;

	/**
	 * Constructs an empty DataParticleBatch with an initial capacity of ten.
	 */
	public DataParticleBatch() {
		super();
	}

	/**
	 * Constructs a DataParticleBatch, in the order they are returned by the
	 * collection's iterator.
	 *
	 * @param c
	 *            the collection whose elements are to be placed into this list
	 * @throws NullPointerException
	 *             if the specified collection is null
	 */
	public DataParticleBatch(Collection<? extends DataParticle> c) {
		super(c);
	}

	/**
	 * Constructs an empty DataParticleBatch with the specified initial
	 * capacity.
	 *
	 * @param initialCapacity
	 *            the initial capacity of the list
	 * @throws IllegalArgumentException
	 *             if the specified initial capacity is negative
	 */
	public DataParticleBatch(int initialCapacity) {
		super(initialCapacity);
	}

}
