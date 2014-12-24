package nl.tno.sensorstorm.api.particles;

import nl.tno.sensorstorm.api.annotation.Mapper;
import nl.tno.sensorstorm.api.annotation.TupleField;

/**
 * Defines a Particle.
 * 
 * Particles are strongly typed classes that can be used in Operations. They
 * always a timestamp. For Serialization they are mapped to Storm Tuples. In
 * order to make this translation, fields that need to be serialized must use
 * the {@link TupleField} annotation. Alternatively, the class can define a
 * custom mapper with the {@link Mapper} annotation.
 * 
 * @author waaijbdvd
 */
public interface Particle {

	/**
	 * Returns the timestamp of this particle
	 * 
	 * @return Returns the timestamp of this particle
	 */
	public long getTimestamp();

	/**
	 * Sets the timestamp of this particle
	 * 
	 * @param timestamp
	 */
	public void setTimestamp(long timestamp);

}