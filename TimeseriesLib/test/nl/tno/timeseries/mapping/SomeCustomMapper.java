package nl.tno.timeseries.mapping;

import java.util.Map;

import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.mapper.api.CustomParticlePojoMapper;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SomeCustomMapper implements
		CustomParticlePojoMapper<SelfMappedParticle> {

	private static Fields fields = new Fields(
			ParticleMapper.TIMESTAMP_FIELD_NAME, "customNameForId", "intId",
			"map");

	@Override
	public Values particleToValues(SelfMappedParticle particle) {
		Values v = new Values();
		v.add(particle.timestamp);
		v.add(particle.id);
		v.add(particle.intId);
		v.add(particle.map);
		return v;
	}

	@SuppressWarnings("unchecked")
	@Override
	public SelfMappedParticle tupleToParticle(Tuple tuple) {
		SelfMappedParticle p = new SelfMappedParticle();
		p.timestamp = tuple.getLongByField(ParticleMapper.TIMESTAMP_FIELD_NAME);
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
