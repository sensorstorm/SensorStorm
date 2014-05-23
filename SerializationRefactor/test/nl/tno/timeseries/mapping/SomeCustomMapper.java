package nl.tno.timeseries.mapping;

import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nl.tno.timeseries.mapper.api.CustomParticlePojoMapper;

public class SomeCustomMapper implements CustomParticlePojoMapper<SelfMappedParticle> {

	private static Fields fields = new Fields("streamId", "sequenceNr", "customNameForId", "intId", "map");

	@Override
	public Values particleToValues(SelfMappedParticle particle) {
		Values v = new Values();
		v.add(particle.streamId);
		v.add(particle.sequenceNr);
		v.add(particle.id);
		v.add(particle.intId);
		v.add(particle.map);
		return v;
	}

	@SuppressWarnings("unchecked")
	@Override
	public SelfMappedParticle tupleToParticle(Tuple tuple) {
		SelfMappedParticle p = new SelfMappedParticle();
		p.streamId = tuple.getStringByField("streamId");
		p.sequenceNr = tuple.getLongByField("sequenceNr");
		p.id = tuple.getStringByField("customNameForId");
		p.intId = tuple.getIntegerByField("intId");
		p.map = (Map<String, Double>) tuple.getValueByField("map");

		return p;
	}

	@Override
	public Fields getFields() {
		return fields;
	}

	@Override
	public boolean canMapTuple(Tuple tuple) {
		return tuple.contains("customNameForId");
	}

}
