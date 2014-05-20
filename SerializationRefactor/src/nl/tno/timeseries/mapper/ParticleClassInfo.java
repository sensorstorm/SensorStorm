package nl.tno.timeseries.mapper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.SortedMap;

import nl.tno.timeseries.mapper.api.Particle;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ParticleClassInfo {


	// key = name of field, value = name in tuple
	private SortedMap<String, String> fields;
	private Fields outputFields;
	private Class<?> clazz;;

	public ParticleClassInfo(Class<?> clazz, SortedMap<String, String> fields) {
		this.fields = fields;
		this.clazz = clazz;
		ArrayList<String> copy = new ArrayList<>();
		copy.add(ParticleMapper.STREAM_ID);
		copy.add(ParticleMapper.SEQUENCE_NR);
		copy.add(ParticleMapper.PARTICLE_CLASS);
		copy.addAll(this.fields.values());
		outputFields = new Fields(copy);
	}

	public <T> T deserialize(Tuple tuple, Class<T> clazz) {
		assert clazz.equals(this.clazz);
		// TODO Auto-generated method stub
		return null;
	}

	public Values serialize(Particle particle) {
		try {
			Values v = new Values();
			v.add(particle.getStreamId());
			v.add(particle.getSequenceNr());
			v.add(particle.getClass());
			for (String field : fields.keySet()) {
				Field declaredField = clazz.getDeclaredField(field);
				declaredField.setAccessible(true);
				v.add(declaredField.get(particle));
			}
			return v;
		} catch (IllegalArgumentException | IllegalAccessException
				| NoSuchFieldException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public Fields getFields() {
		return outputFields;
	}

}
