package nl.tno.timeseries.mapper.api;

import nl.tno.timeseries.mapper.annotation.Mapper;
import nl.tno.timeseries.mapper.annotation.TupleField;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Custom mapper for a Particle
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

	public Values particleToValues(T particle);

	public T tupleToParticle(Tuple tuple);

	public Fields getFields();

	public boolean canMapTuple(Tuple tuple);

}