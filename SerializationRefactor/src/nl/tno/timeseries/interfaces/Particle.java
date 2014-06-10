package nl.tno.timeseries.interfaces;

import nl.tno.timeseries.mapper.annotation.Mapper;
import nl.tno.timeseries.mapper.annotation.TupleField;

/**
 * Defines a Particle.
 * 
 * Particles are strongly typed classes that can be used in Operations. They
 * always contain a channel ID and a sequence number. For Serialization they are
 * mapped to Storm Tuples. In order to make this translation, fields that need
 * to be serialized must use the {@link TupleField} annotation. Alternatively,
 * the class can define a custom mapper with the {@link Mapper} annotation.
 */
public interface Particle {

	public String getChannelId();

	public void setChannelId(String channelId);

	public long getSequenceNr();

	public void setSequenceNr(long sequenceNr);

}