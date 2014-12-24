package nl.tno.sensorstorm.api.particles;

import nl.tno.sensorstorm.api.annotation.Mapper;
import nl.tno.sensorstorm.api.annotation.TupleField;
import nl.tno.sensorstorm.api.processing.Operation;
import backtype.storm.tuple.Tuple;

/**
 * Defines a Particle.
 * 
 * Particles are strongly typed classes that can be used in {@link Operation}s.
 * They always a timestamp. For transportation between {@link Operation}s they
 * are mapped to Storm {@link Tuple}s. In order to make this translation, fields
 * that need to be serialized must use the {@link TupleField} annotation.
 * Alternatively, the class can define a custom mapper with the {@link Mapper}
 * annotation.
 */
public interface Particle {

	/**
	 * @return The timestamp of this particle
	 */
	long getTimestamp();

	/**
	 * Sets the timestamp of this particle.
	 * 
	 * @param timestamp
	 *            new timestamp value
	 */
	void setTimestamp(long timestamp);

}