package nl.tno.timeseries.mapper.api;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public interface CustomParticlePojoMapper<T extends Particle> {

	public Values serialize(T particle);

	public T deserialize(Tuple tuple);

	public Fields getFields();

}
