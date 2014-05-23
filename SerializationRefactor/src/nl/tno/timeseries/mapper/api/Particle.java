package nl.tno.timeseries.mapper.api;

import nl.tno.timeseries.mapper.annotation.Mapper;
import nl.tno.timeseries.mapper.annotation.TupleField;

/**
 * Defines a Particle.
 * 
 * Particles are strongly typed classes that can be used in Operations. They
 * always contain a Stream ID and a sequence number. For Serialization they are
 * mapped to Storm Tuples. In order to make this translation, fields that need
 * to be serialized must use the {@link TupleField} annotation. Alternatively,
 * the class can define a custom mapper with the {@link Mapper} annotation.
 */
public interface Particle {

	public String getStreamId();

	public void setStreamId(String streamId);

	public long getSequenceNr();

	public void setSequenceNr(long sequenceNr);

}