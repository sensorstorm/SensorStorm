package nl.tno.sensorstorm.mapper.api;

import nl.tno.sensorstorm.mapper.annotation.Mapper;
import nl.tno.sensorstorm.mapper.annotation.TupleField;
import nl.tno.sensorstorm.particles.Particle;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Custom mapper for a Particle.
 * 
 * Particles are represented in Storm by dynamically typed Tuples.For
 * transportation Particles are mapped to Tuples. Particles are automatically
 * mapped if they contain the {@link TupleField} annotation. However, in some
 * cases it might be useful to define your own mapper. This class lets you do
 * that. To use this class you have to use the {@link Mapper} annotation in the
 * Particle class.
 * 
 * @param <T>
 *            The Particle implementation mapped by this class.
 */
public interface CustomParticlePojoMapper<T extends Particle> {

	/**
	 * Map a {@link Particle} object into a {@link Values} object.
	 * 
	 * @param particle
	 *            {@link Particle} object to be mapped
	 * @return mapped {@link Particle}
	 */
	Values particleToValues(T particle);

	/**
	 * Map a {@link Tuple} object into a {@link Particle} object.
	 * 
	 * @param tuple
	 *            {@link Tuple} to be mapped
	 * @return mapped {@link Tuple}
	 */
	T tupleToParticle(Tuple tuple);

	/**
	 * @return The {@link Fields} present in this {@link Particle}
	 */
	Fields getFields();

	/**
	 * Check if a {@link Tuple} can be mapped to this type of {@link Particle}.
	 * 
	 * @param tuple
	 *            To be checked
	 * @return True if the {@link Tuple} corresports to this mapper, false
	 *         otherwise
	 */
	boolean canMapTuple(Tuple tuple);

}