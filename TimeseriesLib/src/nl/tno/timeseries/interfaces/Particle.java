package nl.tno.timeseries.interfaces;

import nl.tno.timeseries.mapper.annotation.Mapper;
import nl.tno.timeseries.mapper.annotation.TupleField;

/**
 * Defines a Particle.
 * 
 * Particles are strongly typed classes that can be used in Operations. They
 * always contain a channel ID and a timestamp. For Serialization they are
 * mapped to Storm Tuples. In order to make this translation, fields that need
 * to be serialized must use the {@link TupleField} annotation. Alternatively,
 * the class can define a custom mapper with the {@link Mapper} annotation.
 * 
 * @author waaijbdvd
 */
public interface Particle {

	/**
	 * return the id of the channel the particle belongs to
	 * 
	 * @return Returns the channelid of this particle
	 */
	public String getChannelId();

	/**
	 * Sets the id of the channel the particle belongs to
	 * 
	 * @param channelId
	 */
	public void setChannelId(String channelId);

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